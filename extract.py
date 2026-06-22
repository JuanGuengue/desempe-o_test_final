import requests

# Llamada a la API
response = requests.get("https://randomuser.me/api/")
data = response.json()

# Extraer información
user = data["results"][100]
print("Nombre:", user["name"]["first"], user["name"]["last"])
print("Email:", user["email"])
print("País:", user["location"]["country"])
print("Foto:", user["picture"]["large"])