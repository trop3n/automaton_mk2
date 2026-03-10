#!/bin/bash
#
# Vimeo Classification Cron Script
# Runs the Vimeo classifier every Sunday at 5:00 PM
#
# Installation:
#   crontab -e
#   Add: 0 17 * * 0 /home/jason/dev/projects/automatonmk2/run_classification.sh
#

set -e

# Configuration
SCRIPT_DIR="/home/jason/dev/projects/automatonmk2"
VENV_PATH="$SCRIPT_DIR/.venv"
LOG_DIR="$SCRIPT_DIR/logs"
PYTHON_SCRIPT="$SCRIPT_DIR/automaton_v3.py"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Generate log filename with date
LOG_FILE="$LOG_DIR/classification_$(date +\%Y\%m\%d).log"

# Rotate old logs (keep last 30 days)
find "$LOG_DIR" -name "classification_*.log" -type f -mtime +30 -delete

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Start logging
log "="
log "Vimeo Classification Script Started"
log "="
log ""

# Change to script directory
cd "$SCRIPT_DIR"

# Activate virtual environment
log "Activating virtual environment..."
source "$VENV_PATH/bin/activate"

# Run the classifier
log "Running classifier..."
log ""

if python3 "$PYTHON_SCRIPT" >> "$LOG_FILE" 2>&1; then
    log ""
    log "="
    log "Classification completed successfully"
    log "="
    exit_code=0
else
    exit_code=$?
    log ""
    log "="
    log "ERROR: Classification failed with exit code $exit_code"
    log "="
fi

# Deactivate virtual environment
deactivate

log ""
log "Log file: $LOG_FILE"
log ""

exit $exit_code
