import os, aiohttp, asyncio, sys, signal
from loguru import logger
from dotenv import load_dotenv
from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3
from io import BytesIO

logger.remove()
load_dotenv()

logger.add(
	sys.stdout,
	format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> ! <level>{level}</level> ! {message}",
	colorize=True
)

api_token = os.getenv("API_TOKEN")
chat_id = os.getenv("CHAT_ID")
start_id = int(os.getenv("START_ID"))
end_id = int(os.getenv("END_ID"))
# err_limit = int(os.getenv("ERR_LIMIT"))
# errs = 0

def cd():                                                                       
	print("""
##     ## ##    ## ########  ######## ##       ########    ###     ######  ######## ########  ##     ## ########  ########     ###    ##    ## 
##     ## ###   ## ##     ## ##       ##       ##         ## ##   ##    ## ##       ##     ## ##     ## ##     ## ##     ##   ## ##   ###   ## 
##     ## ####  ## ##     ## ##       ##       ##        ##   ##  ##       ##       ##     ## ##     ## ##     ## ##     ##  ##   ##  ####  ## 
##     ## ## ## ## ########  ######   ##       ######   ##     ##  ######  ######   ##     ## ##     ## ########  ########  ##     ## ## ## ## 
##     ## ##  #### ##   ##   ##       ##       ##       #########       ## ##       ##     ## ##     ## ##   ##   ##     ## ######### ##  #### 
##     ## ##   ### ##    ##  ##       ##       ##       ##     ## ##    ## ##       ##     ## ##     ## ##    ##  ##     ## ##     ## ##   ### 
 #######  ##    ## ##     ## ######## ######## ######## ##     ##  ######  ######## ########   #######  ##     ## ########  ##     ## ##    ## 
	""")

def signalHandler(sig, frame):
	print('\n\nSTOP!')
	sys.exit(0)

signal.signal(signal.SIGINT, signalHandler)

def loadArtists():
	try:
		with open("./data/artists.txt", "r", encoding='utf-8') as f:
			return [line.strip().lower() for line in f if line.strip()]
	except Exception as e:
		logger.error(f"'~/data/artists.txt': {e}")
		return []

def checkSong(artist, artists):
	artist = artist.lower()
	query_name = [name.strip() for name in artist.split(",")]
	return any(name in artists for name in query_name)

async def unreleasedSong(session, albumid):
	try:
		async with session.get("https://ment-backend.mondia.com/structure/pages_music_tracks_details?", params={"id": albumid}, headers={"x-tenant-id": "vf-de"}) as response:
			logger.info(f"unreleasedSong {albumid} ! {response.status}")
			if response.status == 200:
				data = await response.json()
				statusCode = data["substructure"][0]["data"].get("errorCode")
				return statusCode is not None
			else:
				return None
	except Exception as e:
		logger.error(f"unreleasedSong {albumid} ! {e}")
		return None

async def fetchSongs(session, albumid):
	try:
		async with session.get(f"https://p.mondiamedia.com/api/fetch/preview/{albumid}") as response:
			if response.status == 200:
				logger.info(f"Fetching ! {albumid} ! {response.status}")
				data = await response.read()
				mp3_data = BytesIO(data)
				audio = MP3(mp3_data, ID3=EasyID3)
				return {
					"title": audio.get("title", [""])[0],
					"artist": audio.get("artist", [""])[0],
					"album": audio.get("album", [""])[0],
				}
			else:
				logger.warning(f"Fetching ! {albumid} ! {response.status}")
				return None
	except Exception as e:
		logger.error(f"Fetching ! {albumid} ! {e}")
		return None
	
async def send_msg(chat_id, text):
	tlg_api = f"https://api.telegram.org/bot{api_token}/sendMessage"
	payload = {
		"chat_id": chat_id,
		"text": text,
		"parse_mode": "HTML"
	}
	try:
		async with aiohttp.ClientSession() as session:
			async with session.post(tlg_api, json=payload) as response:
				if response.status == 200:
					return True
				logger.info(f"Send unreleasedSong ! {chat_id} ! {response.status}")
				return False
	except Exception as e:
		logger.error(f"{chat_id} ! {e}")
		return False

async def procIds(startid, endid, log, batch_size=50, artists=None):
	# global errs

	if artists is None:
		logger.error("'~/data/artists.txt' empty")

	async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
		for i in range(startid, end_id, batch_size):
			tasks = [
				fetchSongs(session, albumid)
				for albumid in range(i, min(i + batch_size, endid))
			]
			results = await asyncio.gather(*tasks)
			for result, albumid in zip(results, range(i, min(i + batch_size, endid))):
				# if errs >= err_limit:
				#    logger.error(f"Has been received {err_limit} errs")
				#    return None
				
				if result:
					# errs = 0
					title = result.get("title", "")
					artist = result.get("artist", "")
					album = result.get("album", "")

					log.write(f"{albumid}!{artist}!{title}!{album}\n")
					if checkSong(artist, artists) and await unreleasedSong(session, albumid):
						msg_txt = (
							f"<b>ID</b> ! <code>{albumid}</code>"
							f"<b>Song</b> ! <code>{title}</code>\n"
							f"<b>Artist</b> ! <code>{artist}</code>\n"
							f"<b>Album</b> ! <code>{album}</code>"
						)
						await send_msg(chat_id, msg_txt)
				else:
					# errs += 1
					pass

async def parser():
	cd()
	log = open("./data/log.csv", "a", encoding='utf-8')
	artists = loadArtists()
	await procIds(start_id, end_id, log, artists=artists)
	log.close()

if __name__ == "__main__":
	asyncio.run(parser())


# @UnreleasedUrban