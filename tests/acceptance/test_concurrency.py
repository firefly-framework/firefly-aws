from multiprocessing import Pool
from pprint import pprint

import firefly.infrastructure as ffi
import pytest
from firefly_test.todo import TodoList, User

interfaces = ['data_api_pg']


def run_thread(num: int):
    import os
    from pprint import pprint
    from firefly.application.container import Container
    from tests.acceptance.conftest import config
    from dotenv import load_dotenv

    os.environ['ENV'] = 'dev'

    load_dotenv(dotenv_path=os.path.join(os.getcwd(), '../../.env'))
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), '../../.env.dev'))

    c = config()
    print("FUCK")
    pprint(c)
    Container.configuration = lambda self: ffi.MemoryConfigurationFactory()(c)
    container = Container()
    container.kernel.boot()
    container.system_bus.invoke(
        'todo.UpdateTodoListWithSleep', {'id': '9ebe9d14-af67-43df-9596-621f678024f1', 'task_name': f'Task {num}'}
    )


@pytest.mark.parametrize('index', interfaces)
@pytest.mark.skip
def test_concurrency(index, todo_repositories):
    todos = todo_repositories[index]
    todos.migrate_schema()

    t = TodoList(
        id='9ebe9d14-af67-43df-9596-621f678024f1',
        name='My List',
        user=User(name='Foo')
    )
    todos.append(t)
    todos.commit()
    todos.reset()

    with Pool(2) as p:
        p.map(run_thread, [1, 2])

    todos.reset()
    t = todos.find(t.id)
    assert len(t.tasks) == 2
