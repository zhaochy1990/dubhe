package tracing

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed web/*.html web/*.css web/*.js
var tracingAssets embed.FS

func mountTracingUI(mux *http.ServeMux) {
	assets, err := fs.Sub(tracingAssets, "web")
	if err != nil {
		panic(err)
	}
	static := http.StripPrefix("/tracing", http.FileServerFS(assets))
	mux.Handle("GET /tracing/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		static.ServeHTTP(w, r)
	}))
}
