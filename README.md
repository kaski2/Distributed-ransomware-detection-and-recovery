After cloning the repo you need to create a virtual environment:
```
python3 -m venv venv
```

<<<<<<< HEAD
After creating it activate it like so:
Linux:
=======
After this activate it.
Unix:
>>>>>>> ac184c4be0ca5177f0a7295694b4a4d57f39f69d
```
source venv/bin/activate
```

Windows:
```
venv\Scripts\activate
```

Lastly install dependencies with this command:
```
pip install -r requirements.txt
```
Furthermore you need to add the directory you wish to monitor in the settings.ini file created automatically. Example settings.ini:
```
[settings]
monitored_dir_path = <Path to directory>
```