from datetime import datetime

import pytest
from firefly_test.iam.domain.entity import Role, Scope, User as IamUser
from firefly_test.todo import TodoList, User, Task


interfaces = ['data_api_mysql', 'data_api_mysql_mapped']


@pytest.mark.parametrize('index', interfaces)
def test_basic_crud_operations(index, todo_repositories):
    todos = todo_repositories[index]
    todos.migrate_schema()

    todos.append(TodoList(id='abc123', user=User(name='Bob')))
    todos.commit()
    todos.reset()

    assert len(todos) == 1
    todo: TodoList = todos.find('abc123')
    assert todo is not None
    assert todo.user.name == 'Bob'

    todo.tasks.append(Task(name='Task 1', due_date=datetime.now()))
    todos.commit()
    todos.reset()

    assert len(todos) == 1
    todo: TodoList = todos.find('abc123')
    assert len(todo.tasks) == 1

    todo.user.name = 'Phillip'
    todos.commit()
    todos.reset()

    assert len(todos) == 1
    todo: TodoList = todos.find('abc123')
    assert todo.user.name == 'Phillip'

    todos.remove(todo)
    todos.commit()
    todos.reset()

    assert len(todos) == 0
    assert todos.find('abc123') is None


@pytest.mark.parametrize('index', interfaces)
def test_aggregate_associations(index, user_repositories, role_repositories, scope_repositories):
    users = user_repositories[index]
    roles = role_repositories[index]
    scopes = scope_repositories[index]

    users.migrate_schema()
    roles.migrate_schema()
    scopes.migrate_schema()

    my_scopes = [
        Scope(id='foo.admin'),
        Scope(id='bar.admin'),
        Scope(id='foo.Baz.write'),
    ]
    list(map(lambda s: scopes.append(s), my_scopes))
    scopes.commit()

    my_roles = [
        Role(name='Foo Admin', scopes=[my_scopes[0]]),
        Role(name='Bar Admin', scopes=[my_scopes[1]]),
        Role(name='Super Admin', scopes=[my_scopes[0], my_scopes[1]]),
    ]
    list(map(lambda r: roles.append(r), my_roles))
    roles.commit()

    users.append(IamUser(id='john', name='John Doe', email='john@doe.com', roles=[my_roles[0]]))
    users.commit()

    john = users.find('john')

    assert john.roles[0].name == 'Foo Admin'
    assert john.roles[0].scopes[0].id == 'foo.admin'
