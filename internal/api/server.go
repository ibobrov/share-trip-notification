package api

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/asaskevich/govalidator/v12"
	"github.com/gofiber/fiber/v2"
	"github.com/ibobrov/share-trip-notification/internal/service"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	DB                  *pgxpool.Pool
	notificationService *service.NotificationService
}

func NewServer(
	db *pgxpool.Pool,
	service *service.NotificationService,
) *Server {
	return &Server{
		DB:                  db,
		notificationService: service,
	}
}

func ParseAndValidateRequest[T interface{}](request *T, c *fiber.Ctx, logger *slog.Logger) (ok bool, err error) {
	if err := c.BodyParser(request); err != nil {
		logger.Warn(
			"invalid request body",
			slog.String("reason", "invalid_json"), slog.String("result", "validation_error"),
		)
		return false, Failure(c, "некорректное тело запроса")
	}

	if _, err := govalidator.ValidateStruct(*request); err != nil {
		fields := make([]string, 0)
		for field := range govalidator.ErrorsByField(err) {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		validationErrors := make([]string, 0)

		for field, message := range govalidator.ErrorsByField(err) {
			validationErrors = append(validationErrors,
				fmt.Sprintf("field of request `%s` contains error `%s`", field, message),
			)
		}

		// На случай ошибки, которую нельзя привязать к полю.
		if len(validationErrors) == 0 {
			validationErrors = append(validationErrors, err.Error())
		}

		logger.Warn(
			"request validation failed",
			slog.Any("fields", fields), slog.String("result", "validation_error"),
		)

		return false, Failure(c, validationErrors...)
	}

	return true, nil
}
