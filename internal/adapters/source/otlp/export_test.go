package otlp

import "net/http"

const MaxBodyBytes = maxBodyBytes

// HandleLogs serves an OTLP/HTTP logs request without a listening server.
func HandleLogs(o *OTLP, w http.ResponseWriter, r *http.Request) {
	o.handleLogs(w, r)
}
