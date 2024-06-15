# How To Use
1. Set up a virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```
2. Install the requirements
```bash
pip3 install -r requirements.txt
```

3. Use the maturin tool to build the rust library
```bash
maturin dev
```

4. Run the client
```bash
python3 client.py
```