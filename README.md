# Diagho BAM Uploader
Script to automatically upload BAM and MultiQC files to Diagho for Perigenomed project

## Action
Watch a directory for new run folders containing a bam subdir and optional multiqc subdir. Get or create a run on Diagho named as the run folder when a "*.done" file is created in it, then upload the BAM and MultiQC as attachements to that run.
Once all files are loaded, try to link the BAM files to Diagho samples with the same name.

## Quickstart
### Installation
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Launch script
```
nohup python main.py &  
```

### Monitoring
All logs are kept in bam-uploader.log