from loguru import logger as log
log.add("probedge.log", rotation="10 MB", retention="14 days")
