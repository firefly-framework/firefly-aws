import firefly as ff
import pytest


def test_set_get_delete(cache: ff.Cache):
    cache.set('foo', 'bar')
    assert cache.get('foo') == 'bar'
    cache.delete('foo')
    assert cache.get('foo') is None


def test_increment(cache: ff.Cache):
    cache.set('inc_1', 0)
    ret = cache.increment('inc_1')
    assert ret == 1
    assert cache.get('inc_1') == 1
    ret = cache.increment('inc_1')
    assert ret == 2
    assert cache.get('inc_1') == 2
    cache.delete('inc_1')


def test_increment_dict(cache: ff.Cache):
    cache.set('inc_2', {
        'dict': {
            'num': 0,
        }
    })
    ret = cache.increment('inc_2.dict.num')
    assert ret == {'dict': {'num': 1}}
    assert cache.get('inc_2') == {'dict': {'num': 1}}
    ret = cache.increment('inc_2.dict.num', amount=2)
    assert ret == {'dict': {'num': 3}}
    assert cache.get('inc_2') == {'dict': {'num': 3}}
    cache.delete('inc_2')


def test_decrement(cache: ff.Cache):
    cache.set('inc_1', 10)
    ret = cache.decrement('inc_1')
    assert ret == 9
    assert cache.get('inc_1') == 9
    ret = cache.decrement('inc_1')
    assert ret == 8
    assert cache.get('inc_1') == 8
    cache.delete('inc_1')


def test_decrement_dict(cache: ff.Cache):
    cache.set('inc_2', {
        'dict': {
            'num': 10,
        }
    })
    ret = cache.decrement('inc_2.dict.num')
    assert ret == {'dict': {'num': 9}}
    assert cache.get('inc_2') == {'dict': {'num': 9}}
    ret = cache.decrement('inc_2.dict.num', amount=2)
    assert ret == {'dict': {'num': 7}}
    assert cache.get('inc_2') == {'dict': {'num': 7}}
    cache.delete('inc_2')


def test_add_to_list(cache: ff.Cache):
    cache.set('add_1', {
        'set': []
    })
    assert cache.add('add_1.set', 'one') == {'set': ['one']}
    cache.delete('add_1')


def test_remove_from_list(cache: ff.Cache):
    cache.set('remove_1', {
        'set': ['one', 'two', 'three']
    })
    assert cache.remove('remove_1.set', 'two') == {'set': ['one', 'three']}
    cache.delete('remove_1')


@pytest.fixture()
def cache(container):
    return container.cache
