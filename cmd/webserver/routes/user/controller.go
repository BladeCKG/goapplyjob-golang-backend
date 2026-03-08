package user

import (
	"context"

	"goapplyjob-golang-backend/pkg/generated/sqlc"

	"github.com/gin-gonic/gin"
)

type HandlerController struct {
	querier         userQuerier
	userRouterGroup *gin.RouterGroup
}

type userQuerier interface {
	AddUser(context.Context, sqlc.AddUserParams) (int32, error)
	DeleteUser(context.Context, string) (*sqlc.HomepageSchemaUser, error)
	GetUser(context.Context, string) (*sqlc.HomepageSchemaUser, error)
	ListUsers(context.Context) ([]*sqlc.HomepageSchemaUser, error)
}

func NewHandlerController(userRouterGroup *gin.RouterGroup, db userQuerier) *HandlerController {
	return &HandlerController{
		userRouterGroup: userRouterGroup,
		querier:         db,
	}
}

// MockHandlerController introduces method calls that can be implemented on test case basis
type MockHandlerController struct {
	mAdduser    func(context.Context, sqlc.AddUserParams) (int32, error)
	mDeleteUser func(context.Context, string) (*sqlc.HomepageSchemaUser, error)
	mGetUser    func(context.Context, string) (*sqlc.HomepageSchemaUser, error)
	mListUsers  func(context.Context) ([]*sqlc.HomepageSchemaUser, error)
}

func (h MockHandlerController) AddUser(ctx context.Context, arg sqlc.AddUserParams) (int32, error) {
	return h.mAdduser(ctx, arg)
}

func (h MockHandlerController) DeleteUser(ctx context.Context, name string) (*sqlc.HomepageSchemaUser, error) {
	return h.mDeleteUser(ctx, name)
}

func (h MockHandlerController) GetUser(ctx context.Context, name string) (*sqlc.HomepageSchemaUser, error) {
	return h.mGetUser(ctx, name)
}

func (h MockHandlerController) ListUsers(ctx context.Context) ([]*sqlc.HomepageSchemaUser, error) {
	return h.mListUsers(ctx)
}
