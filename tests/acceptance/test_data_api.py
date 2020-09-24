import firefly.tests.rdb_repository as test
import pytest
from firefly import Registry, RepositoryFactory
from firefly_test.iam.domain.entity import Role, Scope, User

interfaces = ['data_api_mysql', 'data_api_mysql_mapped', 'data_api_pg']


@pytest.mark.parametrize('index', interfaces)
def test_basic_crud_operations(index, todo_repositories):
    todos = todo_repositories[index]
    todos.migrate_schema()
    test.test_basic_crud_operations(todos)


@pytest.mark.parametrize('index', interfaces)
def test_aggregate_associations(index, user_repositories, role_repositories, scope_repositories, registry):
    users = user_repositories[index]
    roles = role_repositories[index]
    scopes = scope_repositories[index]

    configure_registry(users, roles, scopes, registry)

    users.migrate_schema()
    roles.migrate_schema()
    scopes.migrate_schema()

    test.iam_fixtures(users, roles, scopes)

    test.test_aggregate_associations(users)


@pytest.mark.parametrize('index', interfaces)
def test_pagination(index, user_repositories, role_repositories, scope_repositories, registry):
    users = user_repositories[index]
    roles = role_repositories[index]
    scopes = scope_repositories[index]

    configure_registry(users, roles, scopes, registry)

    users.migrate_schema()
    roles.migrate_schema()
    scopes.migrate_schema()

    test.iam_fixtures(users, roles, scopes)

    test.test_pagination(users)


@pytest.mark.parametrize('index', interfaces)
def test_list_expansion(index, user_repositories, role_repositories, scope_repositories, registry):
    users = user_repositories[index]
    roles = role_repositories[index]
    scopes = scope_repositories[index]

    configure_registry(users, roles, scopes, registry)

    users.migrate_schema()
    roles.migrate_schema()
    scopes.migrate_schema()

    test.iam_fixtures(users, roles, scopes)

    test.test_list_expansion(users)


def configure_registry(users, roles, scopes, registry: Registry):
    class UserRf(RepositoryFactory):
        def __call__(self, *args, **kwargs):
            return users
    registry.register_factory(User, UserRf())

    class RoleRf(RepositoryFactory):
        def __call__(self, *args, **kwargs):
            return roles
    registry.register_factory(Role, RoleRf())

    class ScopeRf(RepositoryFactory):
        def __call__(self, *args, **kwargs):
            return scopes
    registry.register_factory(Scope, ScopeRf())

    registry.clear_cache()
