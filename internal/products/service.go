package products

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var ErrValidation = errors.New("validation error")

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Create(ctx context.Context, req CreateRequest) (Product, error) {
	if err := validateCreate(req); err != nil {
		return Product{}, err
	}
	return s.repo.Create(ctx, req)
}

func (s *Service) Get(ctx context.Context, id int64) (Product, error) {
	return s.repo.Get(ctx, id)
}

func (s *Service) Update(ctx context.Context, id int64, req UpdateRequest) (Product, error) {
	if err := validateUpdate(req); err != nil {
		return Product{}, err
	}
	return s.repo.Update(ctx, id, req)
}

func (s *Service) Delete(ctx context.Context, id int64) error {
	return s.repo.Delete(ctx, id)
}

func validateCreate(req CreateRequest) error {
	if strings.TrimSpace(req.SKU) == "" {
		return fmt.Errorf("%w: sku is required", ErrValidation)
	}
	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}
	if strings.TrimSpace(req.Category) == "" {
		return fmt.Errorf("%w: category is required", ErrValidation)
	}
	if req.PriceCents < 0 {
		return fmt.Errorf("%w: price_cents must be >= 0", ErrValidation)
	}
	return nil
}

func validateUpdate(req UpdateRequest) error {
	if req.Name != nil && strings.TrimSpace(*req.Name) == "" {
		return fmt.Errorf("%w: name cannot be empty", ErrValidation)
	}
	if req.Category != nil && strings.TrimSpace(*req.Category) == "" {
		return fmt.Errorf("%w: category cannot be empty", ErrValidation)
	}
	if req.PriceCents != nil && *req.PriceCents < 0 {
		return fmt.Errorf("%w: price_cents must be >= 0", ErrValidation)
	}
	return nil
}
