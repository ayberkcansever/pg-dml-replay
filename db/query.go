package db

type DmlQuery struct {
	Query      string           `json:"query"`
	Parameters []QueryParameter `json:"parameters"`
}

type QueryParameter struct {
	Type  int    `json:"type"`
	Value []byte `json:"value"`
}
