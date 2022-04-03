package shopify

type user struct {
	ID               int    `json:"id" isKey:"true"`
	Email            string `json:"email" isHashed:"true"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
	VerifiedEmail    bool   `json:"verified_email"`
	AcceptsMarketing bool   `json:"accepts_marketing"`
}

type item struct {
	Id    string  `json:"id" isKey:"true"`
	Price float64 `json:"price"`
}
type order struct {
	Id        string  `json:"id" isKey:"true"`
	Price     float64 `json:"price"`
	UpdatedAt string  `json:"updated_at"`
}
