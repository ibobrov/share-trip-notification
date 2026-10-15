package api

import (
	"errors"

	"github.com/gofiber/fiber/v2"
	"github.com/ibobrov/share-trip-notification/internal/domain"
)

func HandleError(c *fiber.Ctx, internalServerErrorLogFunc func(), err error) error {
	switch {
	case err == nil:
		return nil

	case errors.Is(err, domain.ErrNotFound):
		return FailureWithHttpStatus(c, fiber.StatusNotFound, err.Error())

	case errors.Is(err, domain.ErrDomain):
		return Failure(c, err.Error())

	default:
		internalServerErrorLogFunc()
		return FailureWithHttpStatus(c, fiber.StatusInternalServerError, err.Error())
	}
}
