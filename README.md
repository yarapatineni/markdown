sequenceDiagram
    autonumber
    participant WF as Databricks Workflow
    participant KV as Key Vault (Secret Scope)
    participant IAS as SAP IAS (OAuth)
    participant SF as SuccessFactors OData v2
    participant BZ as Bronze Delta
    participant SV as Silver (Delta)

    WF->>KV: Load client_id/secret, endpoints
    WF->>IAS: POST /oauth2/token (client_credentials)
    alt 200 OK
        IAS-->>WF: access_token
    else Error
        loop Retry N times (exp backoff + jitter)
            WF->>IAS: retry token
        end
        WF-->>WF: Fail & Alert
        note right of WF: Stop
    end

    loop Page through results
        WF->>SF: GET Entities ($select,$filter,$top, Bearer)
        alt 200 OK
            SF-->>WF: page + optional @odata.nextLink
            WF->>BZ: Append page (+ _load_ts_utc,_source_entity)
            alt Has @odata.nextLink
                WF->>SF: GET nextLink
            else Last page
                break Exit paging
            end
        else 401/403
            WF->>IAS: One-time re-auth
            IAS-->>WF: new access_token
            WF->>SF: Retry request
            note right of WF: If still unauthorized → fail
        else 429/5xx
            WF-->>WF: Retry with backoff; honor Retry-After
            WF->>SF: Retry request
        end
    end

    WF->>SV: MERGE (idempotent upsert), cast types, DQ, SCD
    WF-->>WF: Advance watermark (lastModifiedDateTime)
    WF-->>WF: Success
