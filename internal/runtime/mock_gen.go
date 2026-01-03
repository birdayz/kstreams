package runtime

//go:generate mockgen -destination=mock_runtime_test.go -package=runtime . StringInputProcessor,StringBatchInputProcessor,RawRecordProcessor,BatchRawRecordProcessor,Flusher,Node

// Type aliases for mock generation - mockgen requires concrete types, not generics

// StringInputProcessor is InputProcessor instantiated with string types for testing
type StringInputProcessor = InputProcessor[string, string]

// StringBatchInputProcessor is BatchInputProcessor instantiated with string types for testing
type StringBatchInputProcessor = BatchInputProcessor[string, string]
