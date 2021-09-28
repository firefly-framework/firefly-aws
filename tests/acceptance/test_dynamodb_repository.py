import uuid
from datetime import datetime

import pytest

import firefly as ff
from firefly_test.iam import Scope, Role, User as IamUser
from firefly_test.todo import TodoList, User, Task


@pytest.fixture(scope='session')
def config():
    return {
        'project': 'firefly-aws',
        'provider': 'aws',
        'contexts': {
            'firefly_aws': {
                'region': '${AWS_DEFAULT_REGION}',
                'bucket': 'firefly-aws',
            },
            'todo': {
                'entity_module': 'firefly_test.todo.domain',
                'container_module': 'firefly_test.todo.application',
                'application_module': 'firefly_test.todo.application',
                'storage': {
                    'services': {
                        'ddb': {
                            'type': 'dynamodb',
                            'connection': {
                                'driver': 'dynamodb',
                            },
                        }
                    },
                    'default': 'ddb',
                },
                'extensions': {
                    'firefly_aws': {
                        'environment': {}
                    }
                },
            },
            'iam': {
                'entity_module': 'firefly_test.iam.domain',
                'container_module': 'firefly_test.iam.application',
                'application_module': 'firefly_test.iam.application',
                'storage': {
                    'services': {
                        'ddb': {
                            'type': 'dynamodb',
                            'connection': {
                                'driver': 'dynamodb',
                            },
                        }
                    },
                    'default': 'ddb',
                },
                'extensions': {
                    'firefly_aws': {
                        'environment': {}
                    }
                },
            },
            'calendar': {
                'entity_module': 'firefly_test.calendar.domain',
                'storage': {
                    'services': {
                        'ddb': {
                            'type': 'dynamodb',
                            'connection': {
                                'driver': 'dynamodb',
                            },
                        }
                    },
                    'default': 'ddb',
                },
                'extensions': {
                    'firefly_aws': {
                        'environment': {}
                    }
                },
            },
        },
    }


id_ = str(uuid.uuid4())


def test_basic_crud_operations(registry):
    todos = registry(TodoList)
    todos.append(TodoList(id=id_, user=User(name='Bob')))
    todos.commit()
    todos.reset()

    assert len(todos) == 1
    todo: TodoList = todos.find(id_)
    assert todo is not None
    assert todo.user.name == 'Bob'

    todo.tasks.append(Task(name='Task 1', due_date=datetime.now()))
    todos.commit()
    todos.reset()

    assert len(todos) == 1
    todo: TodoList = todos.find(id_)
    print(todo.to_dict())
    assert len(todo.tasks) == 1

    todo.user.name = 'Phillip'
    todos.commit()
    todos.reset()

    assert len(todos) == 1
    todo: TodoList = todos.find(id_)
    assert todo.user.name == 'Phillip'

    todos.remove(todo)
    todos.commit()
    todos.reset()

    assert len(todos) == 0
    assert todos.find(id_) is None


# def test_aggregate_associations(registry, iam_fixtures):
#     users = registry(User)
#     bob = users.find(lambda u: u.name == 'Bob Loblaw')
#
#     assert bob.roles[0].name == 'Admin User'
#     assert bob.roles[0].scopes[0].id == 'firefly.admin'


def test_mutability(registry):
    users = registry(IamUser)
    subset = users.filter(lambda u: u.name == 'Bob Loblaw')

    assert len(users) == 4
    assert len(subset) == 1
    assert subset[0].name == 'Bob Loblaw'
    assert len(users.filter(lambda u: u.email.is_in(('davante@adams.com', 'bob@loblaw.com')))) == 2
    assert len(users) == 4

    half = users.sort(lambda u: u.name)[2:]
    assert len(half) == 2
    assert half[0].name == 'David Johnson'
    assert half[1].name == 'John Doe'

    bob = users.filter(lambda u: u.name == 'Bob Loblaw')[0]
    bob.email = 'foo@bar.com'
    users.commit()
    assert users.find(lambda u: u.name == 'Bob Loblaw').email == 'foo@bar.com'

    subset = users.filter(lambda u: u.email.is_in(('davante@adams.com', 'bob@loblaw.com'))).sort(lambda u: u.email)
    subset.remove(subset[0])
    users.commit()
    assert len(users) == 3


def test_pagination(users):
    test = users.sort(lambda u: u.name)[0:1]

    assert len(test) == 2
    assert test[0].name == 'Bob Loblaw'
    assert test[1].name == 'Davante Adams'

    test = users.sort(lambda u: u.name)[1:2]

    assert len(test) == 2
    assert test[0].name == 'Davante Adams'
    assert test[1].name == 'David Johnson'

    test = users.sort(lambda u: u.name).filter(lambda u: u.name.is_in(('Davante Adams', 'David Johnson')))
    assert test[0].name == 'Davante Adams'
    assert test[1].name == 'David Johnson'


def test_list_expansion(users):
    my_users = users.filter(lambda u: u.name.is_in(['Bob Loblaw', 'Davante Adams']))

    emails = list(map(lambda u: u.email, my_users))
    emails.sort()
    assert emails == ['bob@loblaw.com', 'davante@adams.com']

    my_users = list(filter(lambda u: u.name == 'Bob Loblaw', users))
    assert len(my_users) == 1
    assert my_users[0].name == 'Bob Loblaw'


@pytest.fixture()
def iam_fixtures(registry):
    users = registry(IamUser)
    roles = registry(Role)
    scopes = registry(Scope)

    my_scopes = [
        Scope(id='firefly.admin'),
        Scope(id='firefly.read'),
        Scope(id='firefly.write'),
    ]
    list(map(lambda s: scopes.append(s), my_scopes))
    scopes.commit()

    my_roles = [
        Role(name='Anonymous User', scopes=[my_scopes[1]]),
        Role(name='Admin User', scopes=[my_scopes[0]]),
        Role(name='Regular User', scopes=[my_scopes[1], my_scopes[2]]),
    ]
    list(map(lambda r: roles.append(r), my_roles))
    roles.commit()

    users.append(User(name='John Doe', email='john@doe.com', roles=[my_roles[2]]))
    users.append(User(name='Bob Loblaw', email='bob@loblaw.com', roles=[my_roles[1]]))
    users.append(User(name='David Johnson', email='david@johnson.com', roles=[my_roles[1], my_roles[2]]))
    users.append(User(name='Davante Adams', email='davante@adams.com', roles=[my_roles[0]]))
    users.commit()

    users.reset()
    roles.reset()
    scopes.reset()
