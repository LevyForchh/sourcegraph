package converter

//
// TODO - use bindata instead?
//

const (
	CreateTableMeta = `
		CREATE TABLE "meta" (
			"id" integer PRIMARY KEY NOT NULL,
			"lsifVersion" text NOT NULL,
			"sourcegraphVersion" text NOT NULL,
			"numResultChunks" integer NOT NULL
		)
	`

	CreateTableDocuments = `
		CREATE TABLE "documents" (
			"path" text PRIMARY KEY NOT NULL,
			"data" blob NOT NULL
		)
	`

	CreateTableResultChunks = `
		CREATE TABLE "resultChunks" (
			"id" integer PRIMARY KEY NOT NULL,
			"data" blob NOT NULL
		)
	`

	CreateTableDefinitions = `
		CREATE TABLE "definitions" (
			"id" integer PRIMARY KEY NOT NULL,
			"scheme" text NOT NULL,
			"identifier" text NOT NULL,
			"documentPath" text NOT NULL,
			"startLine" integer NOT NULL,
			"endLine" integer NOT NULL,
			"startCharacter" integer NOT NULL,
			"endCharacter" integer NOT NULL
		)
	`

	CreateTableReferences = `
		CREATE TABLE "references" (
			"id" integer PRIMARY KEY NOT NULL,
			"scheme" text NOT NULL,
			"identifier" text NOT NULL,
			"documentPath" text NOT NULL,
			"startLine" integer NOT NULL,
			"endLine" integer NOT NULL,
			"startCharacter" integer NOT NULL,
			"endCharacter" integer NOT NULL
		)
	`

	PragmaA = `PRAGMA synchronous = OFF`
	PragmaB = `PRAGMA journal_mode = OFF`

	CreateDefinitionsIndex = `CREATE INDEX "idx_definitions" ON "definitions" ("scheme", "identifier")`
	CreateReferencesIndex  = `CREATE INDEX "idx_references" ON "references" ("scheme", "identifier")`
)
