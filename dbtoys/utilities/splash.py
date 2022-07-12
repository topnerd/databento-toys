"""A text splash module for databento toys. A waste of time to be sure."""
from colorama import Fore
from colorama import Style
from colorama import init

import dbtoys.utilities.logging

# Initialize colorama, required for some platforms.
init()

DBTOYS_SPLASH = f"""{Style.BRIGHT}{Fore.MAGENTA}┏━┳━┓ _| _ _|_ _ {Fore.WHITE}|_  _ __ _|_ _
{Fore.MAGENTA}┃ ┣━┫(_|(_| |_(_|{Fore.WHITE}|_)(/_| | |_(_)
{Fore.MAGENTA}┗━┻━┛{Style.RESET_ALL}{Fore.CYAN} ━━━━━━━━━━━━━━━━━━━━ toys{Fore.RESET}"""

DBTOYS_GOODBYE = (
    f"Log directory: {dbtoys.utilities.logging.DEFAULT_LOG_FILE_PATH}\n"
    "Visit the project at "
    f"{Style.BRIGHT}https://github.com/topnerd/databento-toys{Style.RESET_ALL}"
    "\n\u518d\u89c1"
)
