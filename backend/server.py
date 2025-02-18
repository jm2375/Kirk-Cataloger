from flask import Flask, request, jsonify, Response, stream_with_context, g, render_template, send_from_directory
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from googleapiclient.discovery import build
import musicbrainzngs
import redis
import json
import re
import time
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    CACHE_DURATION = int(os.getenv('CACHE_DURATION', 3600))
    RATE_LIMIT_DAY = os.getenv('RATE_LIMIT_DAY', '200 per day')
    RATE_LIMIT_HOUR = os.getenv('RATE_LIMIT_HOUR', '50 per hour')
    MUSICBRAINZ_DELAY = float(os.getenv('MUSICBRAINZ_DELAY', 1.0))
    MUSICBRAINZ_REQUESTS = int(os.getenv('MUSICBRAINZ_REQUESTS', 20))
    CORS_ORIGINS = os.getenv('CORS_ORIGINS', '*')
    API_BASE_URL = os.getenv('API_BASE_URL', 'http://localhost:5000')

class PlaylistError(Exception):
    def __init__(self, message, status_code=500):
        self.message = message
        self.status_code = status_code

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": Config.CORS_ORIGINS}})
app.config['TEMPLATES_AUTO_RELOAD'] = True

def getRedisClient():
    if not hasattr(g, '_redis_client'):
        g._redis_client = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            decode_responses=True
        )
    return g._redis_client

limiter = Limiter(
    get_remote_address,
    app=app,
    storage_uri=f"redis://{Config.REDIS_HOST}:{Config.REDIS_PORT}",
    default_limits=[Config.RATE_LIMIT_DAY, Config.RATE_LIMIT_HOUR]
)

for path in ['/api/process-playlist', '/api/playlist/']:
    limiter.exempt(lambda p=path: request.path.startswith(p))

musicbrainzngs.set_useragent('Kirk Cataloger', '1.0', os.getenv('MUSICBRAINZ_API'))
musicbrainzngs.set_rate_limit(Config.MUSICBRAINZ_DELAY, Config.MUSICBRAINZ_REQUESTS)

youtubedata = build('youtube', 'v3', developerKey=os.getenv('YOUTUBE_API'))

def filterPlaylistId(url):
    match = re.search(r'list=([a-zA-Z0-9_-]+)', url)
    if not match:
        raise PlaylistError("Invalid YouTube playlist URL", 400)
    return match.group(1)

def filterDate(title):
    match = re.search(r'\b(19|20)\d{2}\b', title)
    if match and 1900 <= int(match.group()) <= 2024:
        return int(match.group())
    return None

def filterType(title):
    if re.search(r'\b(?:Album|LP)\b', title, re.IGNORECASE):
        return 'Album'
    if re.search(r'\bEP\b', title, re.IGNORECASE):
        return 'EP'
    if re.search(r'\bSingle\b', title, re.IGNORECASE):
        return 'Single'
    return 'Unknown'

def artistFormat(data):
    if not data:
        return ''
    return ''.join(credit.get('name', '') + credit.get('joinphrase', '') for credit in data)

def saveCatalog(playlistId, catalog):
    getRedisClient().setex(f"catalog:{playlistId}", Config.CACHE_DURATION, json.dumps(catalog))

def getCatalog(playlistId):
    catalog = getRedisClient().get(f"catalog:{playlistId}")
    return json.loads(catalog) if catalog else None

def clearCatalog(playlistId):
    getRedisClient().delete(f"catalog:{playlistId}")

def saveProgress(playlistId, currentItem=0, totalItems=0, status='inProgress'):
    redisClient = getRedisClient()
    activeConns = redisClient.get(f"connections:{playlistId}")
    redisClient.setex(
        f"progress:{playlistId}",
        Config.CACHE_DURATION,
        json.dumps({
            'status': status,
            'currentItem': currentItem,
            'totalItems': totalItems,
            'timestamp': time.time(),
            'activeConnections': int(activeConns if activeConns else 0)
        })
    )

def getProgress(playlistId):
    progress = getRedisClient().get(f"progress:{playlistId}")
    return json.loads(progress) if progress else None

def clearProgress(playlistId):
    redisClient = getRedisClient()
    redisClient.delete(f"progress:{playlistId}")
    redisClient.delete(f"connections:{playlistId}")
    clearCatalog(playlistId)

def incrementConnections(playlistId):
    return getRedisClient().incr(f"connections:{playlistId}")

def decrementConnections(playlistId):
    redisClient = getRedisClient()
    current = redisClient.decr(f"connections:{playlistId}")
    if current <= 0:
        redisClient.delete(f"connections:{playlistId}")
        progress = getProgress(playlistId)
        if progress and progress['status'] != 'completed':
            clearProgress(playlistId)
    return current

def processPlaylist(playlistId):
    try:
        catalog = playlistData(playlistId)
        if not catalog:
            raise PlaylistError("Failed to fetch playlist data")

        totalItems = len(catalog)
        saveProgress(playlistId, 0, totalItems, 'processing')

        for idx, item in enumerate(catalog):
            progress = getProgress(playlistId)
            if not progress or progress['activeConnections'] <= 0:
                return None

            try:
                if item['ytname'] not in ['Deleted video', 'Private video']:
                    result = musicbrainzngs.search_releases(
                        query=item['ytname'],
                        limit=1,
                        strict=False
                    )

                    if result and 'release-list' in result and result['release-list']:
                        bestMatch = result['release-list'][0]
                        releaseGroup = bestMatch.get('release-group', {})
                        item.update({
                            'score': float(bestMatch.get('ext:score', 0)),
                            'title': bestMatch.get('title', ''),
                            'artist': artistFormat(bestMatch.get('artist-credit', [])),
                            'date': bestMatch.get('date', ''),
                            'type': releaseGroup.get('primary-type', 'Unknown'),
                            'mbid': bestMatch.get('id', '')
                        })

                catalog[idx] = item
                saveCatalog(playlistId, catalog)
                saveProgress(playlistId, idx + 1, totalItems, 'processing')
                time.sleep(Config.MUSICBRAINZ_DELAY)

            except Exception as error:
                continue

        saveProgress(playlistId, totalItems, totalItems, 'completed')
        return catalog

    except Exception as error:
        clearProgress(playlistId)
        return None

def playlistData(playlistId):
    catalog = []
    nextPageToken = None

    try:
        while True:
            playlistRequest = youtubedata.playlistItems().list(
                part='snippet,contentDetails',
                playlistId=playlistId,
                maxResults=50,
                pageToken=nextPageToken
            )
            playlistResponse = playlistRequest.execute()

            for item in playlistResponse['items']:
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                
                entry = {
                    'position': snippet['position'],
                    'score': 0,
                    'title': '',
                    'artist': '',
                    'date': filterDate(snippet['title']),
                    'type': filterType(snippet['title']),
                    'mbid': '',
                    'ytname': snippet['title'],
                    'ytlink': f"https://www.youtube.com/watch?v={contentDetails['videoId']}",
                    'channel': snippet['channelTitle']
                }
                catalog.append(entry)
            
            nextPageToken = playlistResponse.get('nextPageToken')
            if not nextPageToken:
                break

        return catalog
        
    except Exception:
        return None

@app.route('/')
def serve_index():
    return render_template('index.html', api_base_url=Config.API_BASE_URL)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                             'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/api/process-playlist', methods=['POST'])
def startProcess():
    try:
        data = request.get_json()
        if not data or 'playlistUrl' not in data:
            raise PlaylistError('Please provide a playlistUrl', 400)

        playlistId = filterPlaylistId(data['playlistUrl'])
        
        existingCatalog = getCatalog(playlistId)
        existingProgress = getProgress(playlistId)

        if existingCatalog and existingProgress and existingProgress['status'] == 'completed':
            incrementConnections(playlistId)
            return jsonify({
                'success': True,
                'playlistId': playlistId,
                'status': 'completed',
                'data': existingCatalog
            })

        clearProgress(playlistId)
        incrementConnections(playlistId)
        processPlaylist(playlistId)

        return jsonify({
            'success': True,
            'playlistId': playlistId
        })

    except PlaylistError as pe:
        return jsonify({'error': pe.message}), pe.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/playlist/<playlistId>/stream', methods=['GET'])
def streamProcess(playlistId):
    def generate():
        try:
            incrementConnections(playlistId)
            while True:
                progress = getProgress(playlistId)
                catalog = getCatalog(playlistId)

                if not progress:
                    yield 'data: ' + json.dumps({'error': 'Playlist not found'}) + '\n\n'
                    break

                if progress['activeConnections'] <= 0:
                    yield 'data: ' + json.dumps({'error': 'No active connections'}) + '\n\n'
                    break

                if progress['status'] == 'completed' and catalog is not None:
                    yield 'data: ' + json.dumps({
                        'status': 'completed',
                        'data': catalog
                    }) + '\n\n'
                    break

                if progress['status'] == 'processing':
                    yield 'data: ' + json.dumps({
                        'status': 'processing',
                        'current': progress['currentItem'],
                        'total': progress['totalItems'],
                        'data': catalog
                    }) + '\n\n'

                time.sleep(1)

        finally:
            decrementConnections(playlistId)

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )

@app.route('/api/playlist/<playlistId>/status', methods=['GET'])
def getStatus(playlistId):
    progress = getProgress(playlistId)
    if not progress:
        return jsonify({'error': 'Playlist not found'}), 404
    return jsonify(progress)

@app.route('/api/playlist/<playlistId>/cancel', methods=['POST'])
def cancelProcess(playlistId):
    clearProgress(playlistId)
    return jsonify({'success': True, 'message': 'Process cancelled'})

@app.teardown_appcontext
def closeRedis(error):
    if hasattr(g, '_redis_client'):
        g._redis_client.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)