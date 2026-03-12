Garantir um environment na máquina:
sudo apt-get update
sudo apt-get install build-essential python3-dev libffi-dev

apt install python3.10-venv
# Cria o ambiente virtual
python3 -m venv pysus

# Ativa o ambiente (Linux/Mac)
source pysus/bin/activate
# ou no Windows: venv\Scripts\activate

# Atualiza as ferramentas base de compilação do Python
pip install --upgrade pip setuptools wheel



Instale os requirements
pip install -r requirements.txt