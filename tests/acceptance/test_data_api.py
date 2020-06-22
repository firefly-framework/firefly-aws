from datetime import datetime

from firefly_test.todo import TodoList, User, Task


def test_basic_crud_operations(registry):
    todos = registry(TodoList)
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
