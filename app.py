from flask import Flask, abort, jsonify, request

import elasticsearch as ES
#from validate import validate_args

app = Flask(__name__)


@app.route('/')
def index():
    return 'worked'

@app.route('/api/movies/')
def movie_list():
    validate = None#validate_args(request.args)

    if not validate['success']:
        return abort(422)

    defaults = {
        'limit': 50,
        'page': 1,
        'sort': 'id',
        'sort_order': 'asc'
    }

    # Тут уже валидно все
    for param in request.args.keys():
        defaults[param] = request.args.get(param)

   
    # Вот тут, лучше использовать полное условие IF, для повышения читабельности.
    # Уходит в тело запроса. Если запрос не пустой - мультисерч, если пустой - выдает все фильмы
    body = {
        "query": {
            "multi_match": {
                "query": defaults['search'],
                # Если в списке или кортеже 1 элемент, то требуется после него обязательно ставить запятую, иначе список будет опущен. Так же, если этот элемент в дальнейшем не будет изменяться, лучше использовать кортеж.
                "fields": ["title",]
            }
        } # По умолчанию метод get возвращает None, который в условиях приравнивается к False, поэтому смысла изменять значение по умолчанию нет.
    } if defaults.get('search', False) else {} # https://fooobar.com/questions/17904234/what-is-the-difference-between-none-and-false-in-python-3-in-a-boolean-sense

    body['_source'] = dict()
    body['_source']['include'] = ['id', 'title', 'imdb_rating']

    params = {
        # '_source': ['id', 'title', 'imdb_rating'],
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }
    # так же стоит ставить пробелы перед и после знака =
    # лучше использовать with, чтобы гарантировать закрытие соединения. https://pythonworld.ru/osnovy/with-as-menedzhery-konteksta.html
    # так же, стоит вывести параметры для подключения, в отдельную переменную, которую в последующем использовать для подключения.
    # это позволит изменять параметры для подключения в одном месте.
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200},], ) # и тут тоже нет запятой в одноэлементном списке.
    # так же стоит поставить сюда условие для проверки корректности соединения.
    search_res = es_client.search(
        body = body,
        index = 'movies',
        params = params,
        filter_path = ['hits.hits._source',] # тоже самое, отсутствует запятая, в одноэлементном списке.
    )
    es_client.close()

    return jsonify([doc['_source'] for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )

    # Соединение не удалось, и?, пытается все же взять информацию? нужно возвращать ошибку.
    if not es_client.ping():
        print('oh(')
    # опять таки = без пробелов.
    search_result = es_client.get(index = 'movies', id = movie_id, ignore = 404)

    es_client.close()
    # вот тут лучше заменить на not и возвращать ошибку. Чтобы везде был единый стиль. В конце функции возвращаются результаты её выполнения.
    if search_result['found']:
        return jsonify(search_result['_source'])

    return abort(404)

if __name__ == "__main__":
    app.run(host = '0.0.0.0', port = 80) # в задании указано, чтобы был поставлен порт 8000.
