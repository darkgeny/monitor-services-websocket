# import time
# import sys
import json
import warnings
from flask import Flask, jsonify, request
from urllib.parse import unquote

# from flask import FlaskView, route

# main branch growing 1

warnings.filterwarnings('ignore')


app = Flask(__name__)


def get_process_setting(paramname, filterby, valueofthis):
    settings = json.load(open('rest-responder_TEST.json', 'r'))
    return list(filter(lambda x: x[paramname] == filterby, settings))[0][valueofthis]


@app.route('/status', methods=['GET'])
def get_name_node():
    name = unquote(request.args.get('name'))
    st=get_process_setting("name", name, "status")
    return jsonify({name: {'status': st}})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=42001, debug=True)
