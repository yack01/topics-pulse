package handlers

import "net/http"

// CORS allows browser-based clients (e.g. the bundled openapi.html /
// Swagger UI, opened as a local file or from any origin) to call the API
// directly. Safe to leave wide open here since the API has no
// authentication by design (internal tool, see brief section 17).
func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
