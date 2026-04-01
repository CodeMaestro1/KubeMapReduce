package main

import (
	"log"
	"net/http"

	"kubemapreduce/pkg/auth"
)

func main() {

	jwksURL := "http://localhost:8080/realms/mapreduce/protocol/openid-connect/certs"
	issuer := "http://localhost:8080/realms/mapreduce"
	audience := "mapreduce-api"

	validator, err := auth.NewJWTValidator(jwksURL, issuer, audience)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	mux.Handle("/jobs",
		auth.RequireRole("USER", validator, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("User endpoint"))
		})),
	)

	mux.Handle("/admin",
		auth.RequireRole("ADMIN", validator, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("Admin endpoint"))
		})),
	)

	log.Println("API running on :8081")
	log.Fatal(http.ListenAndServe(":8081", mux))
}
