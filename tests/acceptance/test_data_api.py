import uuid

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


@pytest.mark.parametrize('index', interfaces)
def test_large_document(index, user_repositories, role_repositories, scope_repositories, registry, large_document):
    if index == 'data_api_mysql_mapped':
        return

    users = user_repositories[index]
    roles = role_repositories[index]
    scopes = scope_repositories[index]

    configure_registry(users, roles, scopes, registry)

    users.migrate_schema()
    roles.migrate_schema()
    scopes.migrate_schema()

    users.append(large_document)
    users.commit()
    users.reset()

    assert len(users) == 1
    user: User = users.find(id_)
    assert user is not None
    assert len(user.name) == 4194304
    users.reset()

    user: User = users.find(lambda u: u.email == 'foo@bar.com')
    assert user is not None
    assert len(user.name) == 4194304

    user.email = 'bar@baz.com'
    users.commit()
    users.reset()

    user: User = users.find(lambda u: u.email == 'bar@baz.com')
    assert user is not None
    users.reset()

    users.remove(user)
    users.commit()
    users.reset()

    assert len(users) == 0


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


id_ = str(uuid.uuid4())


@pytest.fixture()
def large_document():
    return User(id=id_, email='foo@bar.com', name=('x' * 4194304))
