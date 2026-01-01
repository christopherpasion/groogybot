#!/usr/bin/env python3
"""
Auto-restart supervisor for Discord bot
Automatically restarts the bot if it crashes
"""

import subprocess
import sys
import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MAX_RESTART_ATTEMPTS = 5
RESTART_DELAY = 5  # seconds
COOLDOWN_PERIOD = 60  # seconds before counting resets

def run_bot_with_supervisor():
    """Run bot with automatic restart on failure"""
    restart_count = 0
    last_restart_time = None
    
    # Get bot.py path relative to this script
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    bot_path = os.path.join(script_dir, 'bot.py')
    
    while True:
        try:
            logger.info("=" * 60)
            logger.info(f"Starting bot (Attempt #{restart_count + 1})")
            logger.info("=" * 60)
            
            # Run the bot
            process = subprocess.Popen(
                [sys.executable, bot_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            # Stream output
            for line in iter(process.stdout.readline, ''):
                if line:
                    print(line, end='')
            
            # Wait for process to exit
            exit_code = process.wait()
            
            if exit_code == 0:
                logger.info("Bot exited normally")
                break
            
            # Bot crashed, handle restart
            restart_count += 1
            current_time = time.time()
            
            # Reset counter if cooldown period has passed
            if last_restart_time and (current_time - last_restart_time) > COOLDOWN_PERIOD:
                logger.info("Cooldown period passed, resetting restart counter")
                restart_count = 1
            
            last_restart_time = current_time
            
            if restart_count > MAX_RESTART_ATTEMPTS:
                logger.critical(f"Bot crashed {restart_count} times. Giving up.")
                sys.exit(1)
            
            logger.warning(f"Bot crashed with exit code {exit_code}")
            logger.info(f"Restarting in {RESTART_DELAY} seconds... ({restart_count}/{MAX_RESTART_ATTEMPTS})")
            time.sleep(RESTART_DELAY)
            
        except KeyboardInterrupt:
            logger.info("Supervisor interrupted by user")
            break
        except Exception as e:
            logger.error(f"Supervisor error: {e}")
            restart_count += 1
            if restart_count > MAX_RESTART_ATTEMPTS:
                logger.critical("Too many errors. Giving up.")
                sys.exit(1)
            time.sleep(RESTART_DELAY)

if __name__ == '__main__':
    run_bot_with_supervisor()
