# Agent API (HTTPS)

Base URL (default): `https://127.0.0.1:33551/`

Notes:
- Payloads are JSON unless stated otherwise.
- The agent uses a self-signed TLS certificate generated on first start.
- All endpoints require an auth token (see **Authentication**).
- Agent config is stored on disk as YAML (`agent.yaml`).

## Authentication

Every request must include the agent token, either as:
- `Authorization: Bearer <token>`
- `x-agent-token: <token>`

If the token is missing/invalid the agent responds with:
```json
{"error":"unauthorized"}
```

# Virt Backup Account API (HTTPS)

Base URL: `https://virtbackup.net/`

Local development hostname checks for Ronald's Mac mini and Linux host `nuc04` are kept in the app, but account login uses `https://virtbackup.net/`.

The desktop app uses this API only for account sign-in status. It does not gate backup, restore, schedule, or storage functionality.

## Browser Account Login

The desktop app signs in through the system browser. The app starts a temporary local HTTP callback server and opens:

- `GET /app-login?redirect_uri=http://127.0.0.1:<port>/auth/callback&state=<random>&code_challenge=<S256-PKCE-challenge>`

After opening a Virt Backup account link, the app always shows a dialog with a copy-link action. The full URL is not shown in the UI; users can copy it to the clipboard and paste it into a browser on any platform.

If the browser is not signed in, the website sends the user through the normal website login page. After a successful website login, the backend redirects to the app callback:

- `http://127.0.0.1:<port>/auth/callback?code=<one-time-code>&state=<same-state>`

The app verifies `state` before exchanging the code. The app also keeps the PKCE `code_verifier` that produced `code_challenge`; the backend requires that verifier during code exchange.

Browser-login codes and their PKCE challenges are stored in `public.app_login_codes` so `/app-login` and `/api/auth/exchange` can run on different backend pods. Codes expire after 2 minutes and are removed when exchanged.

The backend stores each issued web or desktop-app session in `public.account_sessions`. Tokens contain a `sid` and are accepted only while that database session is active and not expired or revoked.
Desktop-app sessions use expiring access tokens plus rotating refresh tokens. Website sessions keep the normal expiring cookie lifetime.

## Account Code Exchange

- `POST /api/auth/exchange`

Request:
```json
{"code":"one-time-code","codeVerifier":"pkce-code-verifier","debugAccessToken":false}
```

`debugAccessToken` is only sent by the Flutter GUI in debug builds. It makes the issued agent access token valid for 15 minutes instead of 7 days, and refreshes for that session keep using the same 15-minute lifetime. The agent does not need to run in debug mode for this.

Errors are JSON objects with an `error` code, for example:
```json
{"error":"invalid_code"}
```

Response:
```json
{
  "email": "user@example.com",
  "sessionToken": "...",
  "accessToken": "...",
  "accessTokenExpiresAt": "2026-05-17T10:00:00.000Z",
  "refreshToken": "...",
  "refreshTokenExpiresAt": "2026-06-09T10:00:00.000Z"
}
```

The desktop agent stores `accessToken` and `refreshToken` in `agent.yaml` using the same AES-GCM encryption used for other credentials. Access tokens are valid for 7 days, or 15 minutes when the GUI performs login from a debug build. Refresh tokens are valid for 30 days and rotate on every refresh.

Known error codes:
- `code_required`
- `code_verifier_required`
- `invalid_code`
- `server_error`

## Account Session

- `GET /api/auth/session`

Authentication:
- `Authorization: Bearer <sessionToken>`

Response:
```json
{"email":"user@example.com"}
```

Invalid or expired tokens return:
```json
{"error":"unauthorized"}
```

## Account Refresh

- `POST /api/auth/refresh`

Request:
```json
{"refreshToken":"refresh-token"}
```

Response:
```json
{
  "email": "user@example.com",
  "sessionToken": "...",
  "accessToken": "...",
  "accessTokenExpiresAt": "2026-05-17T10:00:00.000Z",
  "refreshToken": "...",
  "refreshTokenExpiresAt": "2026-06-09T10:00:00.000Z"
}
```

The agent schedules refresh at roughly two-thirds of the access-token lifetime. Reusing an already rotated refresh token revokes the session.

## Account Logout

- `POST /api/auth/logout`

Authentication:
- `Authorization: Bearer <sessionToken>`

Response:
```json
{"success":true}
```

The session identified by the bearer token is marked as revoked in the database.

## Health

- `GET /health`

Response:
```json
{"ok":true,"nativeSftpAvailable":true}
```

## Drivers

- `GET /drivers`

Response: array of available storage drivers and their capabilities/params.
```json
[
  {
    "id": "filesystem",
    "label": "Filesystem",
    "usesPath": true,
    "capabilities": {
      "supportsRangeRead": true,
      "supportsBatchDelete": true,
      "supportsMultipartUpload": false,
      "supportsServerSideCopy": false,
      "supportsConditionalWrite": false,
      "supportsVersioning": false,
      "maxConcurrentWrites": 16,
      "params": []
    }
  }
]
```

## Config

### Get full config

- `GET /config`

Response:
```json
{
  "backupPath":"/var",
  "log_level":"info",
  "backupStorageId":"dest_filesystem_1739440000000000",
  "connectionVerified":false,
  "blockSizeMB":1,
  "dummyDriverTmpWrites":false,
  "maxConcurrentBackupRestoreJobs":1,
  "maxConcurrentJobsPerVm":1,
  "maxConcurrentJobsPerStorage":1,
  "ntfymeToken":"",
  "servers":[],
  "storage":[
    {
      "id":"dest_filesystem_1739440000000000",
      "name":"Local filesystem",
      "driverId":"filesystem",
      "enabled":true,
      "params":{"path":"/var"}
    },
    {
      "id":"dest_sftp_1739440000000001",
      "name":"Remote SFTP",
      "driverId":"sftp",
      "enabled":true,
      "disableFresh":true,
      "params":{
        "host":"my-sftp.example.com",
        "port":22,
        "username":"backup",
        "password":"",
        "basePath":"/Backup"
      }
    }
  ],
  "schedules":[
    {
      "id":"schedule_1739440000000002",
      "name":"Nightly VM backup",
      "enabled":true,
      "waitForRunningJobs":true,
      "type":"backup",
      "frequency":"every5Minutes",
      "time":"00:02",
      "weekdays":[],
      "serverId":"server_1739440000000003",
      "storageId":"dest_sftp_1739440000000001",
      "vmName":"app01",
      "restoreXmlPath":"",
      "restoreDecision":""
    },
    {
      "id":"schedule_1739440000000004",
      "name":"Weekly restore drill",
      "enabled":false,
      "waitForRunningJobs":false,
      "type":"restore",
      "frequency":"weekly",
      "time":"09:00",
      "weekdays":[1],
      "serverId":"server_1739440000000005",
      "storageId":"dest_sftp_1739440000000001",
      "vmName":"app01",
      "restoreXmlPath":"__latest__",
      "restoreDecision":"overwrite"
    }
  ]
}
```

### Schedules

Schedules are stored in `agent.yaml` under `schedules` grouped by the local agent hostname. Only schedules are grouped this way; servers, storage, tokens, and guard settings remain shared root config.

```yaml
schedules:
  nuc04:
    - id: schedule_1739440000000002
      name: Backup app01 on nuc04 to Remote SFTP
      enabled: true
      waitForRunningJobs: true
      type: backup
      frequency: every5Minutes
      time: '00:02'
      weekdays: []
      serverId: server_1739440000000003
      storageId: dest_sftp_1739440000000001
      vmName: app01
      restoreXmlPath: ''
      restoreDecision: ''
```

`GET /config` and `POST /config` still use the flat schedule list for the current agent.
The agent checks enabled schedules every 30 seconds and starts matching backup or restore jobs at the configured local agent time.
Backup and restore starts are guarded by `maxConcurrentBackupRestoreJobs`, `maxConcurrentJobsPerVm`, and `maxConcurrentJobsPerStorage`. All three are stored in `agent.yaml`, default to `1`, and also apply to manual schedule runs. When a due schedule is blocked by a guard, the agent records a failed job and sends the configured ntfyme failure notification.

Fields:
- `name`: generated from schedule type, server, storage, and VM. Clients should not expose this as an editable field.
- `waitForRunningJobs`: when `true`, an automatic schedule run that is blocked by a concurrency guard remains pending and starts when the guard allows it. When `false`, the blocked run is recorded as a failed job and sends the configured ntfyme failure notification.
- `type`: `backup` or `restore`.
- `frequency`: `every5Minutes`, `hourly`, `daily`, or `weekly`.
- `time`: local agent time in `HH:mm` format. Hourly schedules use the minute portion and run every hour on that minute. `every5Minutes` schedules also use the minute portion as an offset, for example `00:02` runs at `:02`, `:07`, `:12`, and so on.
- `weekdays`: ISO weekday numbers (`1` Monday through `7` Sunday), used only for weekly schedules.
- `serverId` and `storageId`: references to configured server and storage entries.
- Backup schedules use `vmName`.
- Restore schedules use `restoreXmlPath` and `restoreDecision`. Set `restoreXmlPath` to `__latest__` and `vmName` to a source VM name to restore the latest complete XML for that VM at runtime.

### Run schedule now

- `POST /schedules/{id}/run`

Starts the schedule immediately, even when `enabled` is `false`.

Response:
```json
{"jobId":"1739440000000-backup"}
```

The GUI schedules list includes quick filters for server, backup/restore type, server-VM combination, and storage. These filters are local UI state and are not stored in `agent.yaml`.
Rows for schedules with a running job are highlighted in the GUI. The agent includes `scheduleId` in job status responses for jobs started from a schedule.

### Update full config

- `POST /config`

Body: same shape as `GET /config`.

Response:
```json
{"success":true}
```

## SFTP

### Test connection

- `POST /sftp/test`

Notes:
- The agent will create and use a `VirtBackup` folder inside `basePath`.

Body:
```json
{"host":"my-sftp.example.com","port":22,"username":"backup","password":"...","basePath":"/Backup"}
```

Response:
```json
{"success":true,"message":"SFTP connection successful (read/write OK)."}
```

## Events (SSE)

- `GET /events`

Response: `text/event-stream` (Server-Sent Events).

Initial event:
```
event: ready
data: ok
```

Subsequent events use:
- `event: <type>`
- `data: {"type":"<type>","payload":{...}}`

Currently emitted event types:
- `vm.lifecycle`
- `agent.job_failure`

## Notifications (Ntfy me)

- `POST /ntfyme/test`

Body (optional `token`; if missing, the agent uses `ntfymeToken` from config):
```json
{"token":"<ntfyme token>"}
```

Response (success):
```json
{"success":true,"message":"Test notification delivered.","statusCode":200}
```

## OAuth

### Store Google OAuth tokens

- `POST /oauth/google`

Body:
```json
{
  "storageId":"dest_gdrive_1739440000000002",
  "accessToken":"<access token>",
  "refreshToken":"<refresh token>",
  "scope":"https://www.googleapis.com/auth/drive.file",
  "accountEmail":"user@example.com",
  "expiresAt":1775707200000
}
```

Notes:
- `storageId` is required and must reference a storage with `driverId: "gdrive"`.
- `refreshToken` is required.
- `expiresAt` may be a Unix timestamp in seconds or milliseconds, or an ISO-8601 string.

Response:
```json
{"success":true}
```

### Clear Google OAuth tokens

- `POST /oauth/google/clear`

Body:
```json
{"storageId":"dest_gdrive_1739440000000002"}
```

Response:
```json
{"success":true}
```

## Servers

### List VM status (cached)

- `GET /servers/{serverId}/vms`

Response (array):
```json
[
  {
    "vm":{"id":"vm1","name":"vm1","powerState":"running"},
    "hasOverlay":false
  }
]
```

### Refresh server inventory (manual)

- `POST /servers/{serverId}/refresh`

Response:
```json
{"success":true}
```

### Test SSH connection

- `POST /servers/{serverId}/test`

Body:
```json
{}
```

Response:
```json
{"success":true}
```

### VM action

- `POST /servers/{serverId}/actions`

Body:
```json
{"action":"start|reboot|shutdown|forceReset|forceOff","vmName":"my-vm"}
```

Response:
```json
{"success":true}
```

### Cleanup overlays

- `POST /servers/{serverId}/cleanup`

Body:
```json
{"vmName":"my-vm"}
```

Response:
```json
{"success":true}
```

### Start backup job

- `POST /servers/{serverId}/backup`

Body:
```json
{
  "vmName":"my-vm",
  "storageId":"dest_filesystem_1739440000000000",
  "blockSizeMB":4,
  "fresh":false
}
```

Notes:
- `storageId` selects one configured storage from root-level `storage`.
- `blockSizeMB` is optional and overrides dedup block size for this backup job only (`1`, `2`, `4`, `8`).
- For backward compatibility, `driverId` and `driverParams` are still accepted when present.
- The filesystem storage path is resolved from `storage[id=filesystem].params.path`.
- `fresh: true` triggers a driver "fresh cleanup" (driver-specific behavior). This is only executed when the agent runs in debug mode.
- `disableFresh: true` on a storage forces `fresh` off for that storage; the backup still starts and the agent logs that `fresh` was ignored.

Response:
```json
{"jobId":"<job-id>"}
```

## Jobs

### List jobs

- `GET /jobs`

Response (array):
```json
[{"id":"<job-id>","type":"backup","state":"running","message":"","totalUnits":0,"completedUnits":0,"bytesTransferred":0,"speedBytesPerSec":0}]
```

### Get job status

- `GET /jobs/{jobId}`

Response (subset):
```json
{
  "id":"<job-id>",
  "type":"backup|restore|sanity",
  "state":"running|success|failure|canceled",
  "message":"...",
  "totalUnits":0,
  "completedUnits":0,
  "bytesTransferred":0,
  "speedBytesPerSec":0
}
```

Additional fields may be present (physical throughput, ETA, and writer backlog metrics).

### Cancel job

- `POST /jobs/{jobId}/cancel`

Response:
```json
{"success":true}
```

## Restore

### List restore entries

- `GET /restore/entries`

Required query:
- `storageId=<id>`: list entries for a specific storage.

Optional query:
- `driverId=<id>`: override driver for compatibility/debug scenarios.

Response (array):
```json
[
  {
    "xmlPath":"/path/to/backup.xml",
    "vmName":"my-vm",
    "timestamp":"2026-01-30T12:00:00-4MB",
    "diskBasenames":["disk1.qcow2"],
    "missingDiskBasenames":[],
    "blockSizeMbValues":[4],
    "sourceServerId":"server-id",
    "sourceServerName":"Server"
  }
]
```

### Precheck

- `POST /servers/{serverId}/restore/precheck`

Body:
```json
{"xmlPath":"/path/to/backup.xml","storageId":"dest_filesystem_1739440000000000"}
```

Response:
```json
{"vmExists":true,"canDefineOnly":false}
```

### Start restore job

- `POST /servers/{serverId}/restore/start`

Body:
```json
{"xmlPath":"/path/to/backup.xml","decision":"overwrite|define","storageId":"dest_filesystem_1739440000000000"}
```

Notes:
- `storageId` selects one configured storage for restore reads.
- For backward compatibility, `driverId` is still accepted.

Response:
```json
{"jobId":"<job-id>"}
```

### Sanity Check (verify manifests/blobs)

- `POST /restore/sanity`

Body:
```json
{"xmlPath":"/path/to/backup.xml","timestamp":"2026-01-30T12:00:00-4MB","storageId":"dest_filesystem_1739440000000000"}
```

Response:
```json
{"jobId":"<job-id>"}
```

Notes:
- Full check computes blob SHA-256 via `virtbackup_native` (native library required on the agent host).

### Quick Check (verify blob presence via cached directory scans)

- `POST /restore/quick-check`

Body:
```json
{"xmlPath":"/path/to/backup.xml","timestamp":"2026-01-30T12:00:00-4MB","storageId":"dest_filesystem_1739440000000000"}
```

Response:
```json
{"jobId":"<job-id>"}
```

### Delete restore manifests

- `POST /restore/manifests/delete`

Body:
```json
{"xmlPath":"/path/to/backup.xml","timestamp":"2026-01-30T12:00:00-4MB","storageId":"dest_filesystem_1739440000000000"}
```

Response:
```json
{"success":true,"deletedCount":2}
```

Notes:
- Deletes only manifest files for the selected restore timestamp.
- Blob data is not deleted.
