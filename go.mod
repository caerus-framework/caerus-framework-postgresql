module github.com/caerus-framework/caerus-framework-postgresql

go 1.26

replace github.com/caerus-framework/caerus-framework => ../caerus-framework

require (
	github.com/caerus-framework/caerus-framework v0.0.5
	github.com/caerus-framework/caerus-framework-logs v0.0.2
	github.com/golang-migrate/migrate/v4 v4.19.1
	github.com/jackc/pgx/v5 v5.9.2
)

require (
	github.com/caerus-framework/caerus-framework-configuration v0.0.2
	github.com/caerus-framework/caerus-framework-observability v0.1.0
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/text v0.31.0 // indirect
)

replace github.com/caerus-framework/caerus-framework-logs => ../caerus-framework-logs

tool github.com/caerus-framework/caerus-framework/cmd/caerusvet
