import config
from pymongo import MongoClient
import certifi
import dns.resolver
import requests
import json
import time
import gzip
import shutil
import os
import pandas as pd
import os.path
import base64
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers = ['8.8.8.8']

client = MongoClient(f'mongodb+srv://{config.mongo_pat}', tlsCAFile=certifi.where())
db = client['legacy-api-management']
col_soc = db["societies"]
col_user = db["users"]

def get_all():
    result = col_soc.aggregate([
        {
            '$match': {
                "status": 0,
            }
        }, {
            '$unwind': '$members'
        }, {
            '$project': {
                'members': '$members.user',
                'roles': '$members.roles',
                'billing': '$members.billing',  # Ajout du champ billing pour interroger la collection billings
                '_id': 1,
                'name': 1,
                "createdAt": 1
            }
        }, {
            '$lookup': {
                'from': 'users',
                'localField': 'members',
                'foreignField': '_id',
                'as': 'userDetails'
            }
        }, {
            '$unwind': '$userDetails'
        },
        {
            '$lookup': {
                'from': 'billings',
                'localField': 'billing',  # Champ billing des membres
                'foreignField': 'id',  # Correspond au champ 'id' dans la collection billings
                'as': 'billing_info'
            }
        }, {
            '$unwind': {
                'path': '$billing_info',
                'preserveNullAndEmptyArrays': True  # Pour les cas où il n'y a pas de correspondance
            }
        }, {
            '$project': {
                '_id': 1,
                'name': 1,
                'orga_createdAt': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$createdAt'}},
                'email': '$userDetails.email',
                'firstname': '$userDetails.firstname',
                'lastname': '$userDetails.lastname',
                'user_id': {'$toString': '$userDetails._id'},
                'user_status': '$userDetails.status',
                'user_no_acces': '$userDetails.unaccessible',
                'user_role': {'$arrayElemAt': ['$roles', 0]},
                'title': '$userDetails.title',
                'language': '$userDetails.settings.language',
                'statususer': '$userDetails.settings.statususer',
                'user_created': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$userDetails.createdAt'}},
                'user_updated': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$userDetails.updatedAt'}},
                'billing_raison': '$billing_info.raison'  # Ajout du champ 'raison' récupéré de la collection 'billings'
            }
        }
    ])

    data = []

    for r in result:
        email = r['email']
        user_id = r['user_id']
        orga_createdAt = r['orga_createdAt']
        try:
            language = r['language']
        except:
            language = ""
        orga_name = r['name'].upper()
        orga_id = r['_id']
        user_role = r['user_role']
        user_no_acces = r['user_no_acces']
        user_updated = r['user_updated']
        billing_raison = r.get('billing_raison',
                               'nc')  # Récupération du champ 'raison', avec une valeur par défaut 'nc'

        if user_no_acces == False:
            user_no_acces = "AVEC"
        elif user_no_acces == True:
            user_no_acces = "SANS"
        else:
            user_no_acces = "nc"

        try:
            user_statususer = r['statususer']
        except KeyError:
            user_statususer = "nc"

        title = r['title']
        if title == "Mr":
            title = "Monsieur"
        elif title == "Mrs":
            title = "Madame"
        else:
            title = "nc"

        user_status = r['user_status']
        if user_status == 0:
            user_status = "ACTIF"
        elif user_status == -1:
            user_status = "DESACTIVE"
        else:
            user_status = "nc"

        firstname = r['firstname'].capitalize()
        lastname = r['lastname'].upper()
        user_created = r['user_created']

        # Ajout du champ 'billing_raison' dans le dictionnaire
        data.append({
            'email': email,
            'external_id': user_id,
            'language': language,
            'company': orga_name,
            'company_id': orga_id,
            'titre': title,
            'prenom': firstname,
            'user_role': user_role,
            'user_access_plateforme': user_no_acces,
            'user_statut': user_status,
            'user_date_update': user_updated,
            'billing': billing_raison  # Ajout du champ raison à l'export
        })

    df = pd.DataFrame(data)
    df.to_csv("/Users/patrick/PycharmProjects/one/csv/results/base.csv")

def get_portefeuille():
    print("########### GET PORTEFEUILLE START ###########")

    import requests

    FILTER_ID = 1514

    url = f"https://api.pipedrive.com/v1/organizations?filter_id={FILTER_ID}&limit=500&api_token={config.api_pipedrive}"

    payload = {}
    headers = {
        'Accept': 'application/json',
        'Cookie': '__cf_bm=epS6IiqbeFLh_ZfXxyo.l824MgGVUnpX._S9_Ntj1KA-1693828120-0-AQzPhvtpyCNHrbv5xvIoWIigXcIjKapnnyOvpRQvT2AYGIPIxxr1Yj2+pOp9aj77yICnSx0589w/ZDQX4syB9NU='
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    response = (response.json()['data'])
    l_society_id = []
    l_inactif = []
    l_golive = []
    inac = [("763", "ACTIF"), ("755", "ACTIF"), ("746", "ACTIF"), ("747", "INACTIF"), ("749", "TEST"),
            ("750", "TEST"), ("748", "INACTIF"), ("751", "INACTIF")]

    for i in response:
        societyId = (i['9d0760fac9b60ea2d3f590d3146d758735f2896d'])
        inactif = (i['a056613671b057f83980e4fd4bb6003ce511ca3d'])
        golive = str(i['24582ea974bfcb46c1985c3350d33acab5e54246'])[:7]

        for a, b in inac:
            if inactif == a:
                inactif = b

        l_society_id.append(societyId)
        l_inactif.append(inactif)
        l_golive.append(golive)
    df = pd.DataFrame({'company_id': l_society_id, "company_statut": l_inactif, "company_golive": l_golive})

    for col in df.select_dtypes(include=['float64', 'int64']):
        df[col] = df[col].astype(str)

    df.to_csv("/Users/patrick/PycharmProjects/one/csv/results/pipe_all.csv")

    df1 = pd.read_csv("/Users/patrick/PycharmProjects/one/csv/results/base.csv")
    merged_df = pd.merge(df1, df, on='company_id', how='left')
    merged_df.fillna('nc', inplace=True)
    # merged_df = merged_df.drop(columns=['companie_id'])

    merged_df.to_csv("/Users/patrick/PycharmProjects/one/csv/results/baseXpipe.csv")

def onesig_merge(filename):
    import json
    import pandas as pd

    # Lire les données de df1
    df1 = pd.read_csv(f'/Users/patrick/PycharmProjects/one/csv/todo/{filename}')



    # Lire les données de df0
    df0 = pd.read_csv(f'/Users/patrick/PycharmProjects/one/csv/results/baseXpipe.csv')

    # Identifier les colonnes communes
    common_columns = df0.columns.intersection(df1.columns)

    # Supprimer les colonnes communes de df1
    df1 = df1.drop(columns=common_columns)

    # Ajouter le préfixe 'old_' aux colonnes de df1
    df1 = df1.add_prefix('old_')

    # Concaténer les deux DataFrames
    df_merged = pd.concat([df0, df1], axis=1)

    # Supprimer les colonnes spécifiées
    df_merged = df_merged.loc[:, ~df_merged.columns.str.contains('old_|Unnamed')]

    # Renommer la colonne 'old_subscribed' en 'subscribed'
    # df_merged = df_merged.rename(columns={
    #     "companie":"company",
    #     "companie_statut":"company_statut",
    #     "companie_golive":"company_golive"
    # })
    # df_merged['companie_id'] = None  # ou pd.NA pour NaN

    # df_merged.drop(columns="companie_id",inplace=True)
    # Sauvegarder le DataFrame fusionné
    from datetime import datetime

    now = datetime.now()

    # Extract the date, hour, and minutes
    current_date = now.strftime("%Y%m%d")  # Format as YYYY-MM-DD
    current_time = now.strftime("%H%M")  # Format as HH:MM

    df_merged.to_csv(f'/Users/patrick/PycharmProjects/one/csv/results/onesig_import_{current_date}_{current_time}.csv', sep=';', index=False)

def create_subs():
    df = pd.read_csv('base.csv')
    for d in range (len(df)):
        user_id = df['external_id'][d]
        email = df['email'][d]
        language = df['language'][d]
        orga_name = df['company'][d]
        title = df['titre'][d]
        firstname = df['prenom'][d]
        lastname = df['nom'][d]
        user_role = df['role'][d]
        user_no_acces = df['acces_plateforme'][d]
        # user_statususer = df['etat_activation'][d]

        url = f"https://api.onesignal.com/apps/{config.api_onesig}/users"

        payload = {
            "identity": {"external_id": f"{user_id}"},
            "subscriptions": [
                {
                    "type": "Email",
                    "token": f"{email}",
                    "enabled": True
                }
            ],
            "properties": {
                "language": f"{language}",
                "tags": {
                "companie": f"{orga_name}",
                "titre": f"{title}",
                "prenom": f"{firstname}",
                "nom": f"{lastname}",
                "role": f"{user_role}",
                # "date_creation": f"{user_created}",
                # "statut": f"{user_status}",
                "acces_plateforme": f"{user_no_acces}",
                # "etat_activation": f"{user_statususer}",
            }
        }
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)

def create_one():
    user_id = "5f90021b30da314e10f380ea"
    email = "patrick.phimvilayphone@supertripper.com"
    language = 'fr'

    orga_name = "Supertripper"
    user_role = "SuperHero"
    user_no_acces = "false"
    user_statususer = "CREATED"
    title = "Mr"
    user_status = 0
    firstname = "Patrick"
    lastname = "PHIMVILAYPHONE"
    user_created = "2020-10-21"

    url = f"https://api.onesignal.com/apps/{config.api_onesig}/users"

    payload = {
        "identity": {"external_id": f"{user_id}"},
        "subscriptions": [
            {
                "type": "Email",
                "token": f"{email}",
                "enabled": True
            }
        ],
        "properties": {
            "language": f"{language}",
            "tags": {
            "companie": f"{orga_name}",
            "titre": f"{title}",
            "prenom": f"{firstname}",
            "nom": f"{lastname}",
            "role": f"{user_role}",
            "date_creation": f"{user_created}",
            "statut": f"{user_status}",
            "acces_plateforme": f"{user_no_acces}",
            "etat_activation": f"{user_statususer}",
        }
    }
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)

def signal_one():
    user_id = "5f90021b30da314e10f380ea"
    email = "patrick.phimvilayphone@supertripper.com"
    language = 'fr'

    orga_name = "Supertripper"
    user_role = "SuperHero"
    user_no_acces = "false"
    user_statususer = "CREATED"
    title = "Mr"
    user_status = 0
    firstname = "Patrick"
    lastname = "PHIMVILAYPHONE"
    user_created = "2020-10-21"

    url = f"https://api.onesignal.com/apps/{config.api_onesig}/users"

    payload = {
        "identity": {"external_id": f"{user_id}"},
        "subscriptions": [
            {
                "type": "Email",
                "token": f"{email}",
                "enabled": True
            }
        ],
        "properties": {
            "language": f"{language}",
            "tags": {
                "company": f"{orga_name}",
                "title": f"{title}",
                "firstname": f"{firstname}",
                "lastname": f"{lastname}",
                "user_created": f"{user_created}",
                "role": f"{user_role}",
                "no_access": f"{user_no_acces}",
                "status_user": f"{user_statususer}",
                "active": f"{user_status}",
            }
        }
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)

def signal_unsub() :
    import requests
    import pandas as pd

    df = pd.read_csv('csv/todo/todel.csv')

    for i in range(len(df)):
        player_id = (df['external_id'][i])
        # url = f"https://api.onesignal.com/apps/{config.api_onesig}/subscriptions/{player_id}"
        url = f"https://api.onesignal.com/apps/{config.api_onesig}/users/by/external_id/{player_id}"

        headers = {"accept": "application/json"}

        response = requests.delete(url, headers=headers)

    #

def export():
    # Step 1: Trigger the export
    url = f"https://api.onesignal.com/players/csv_export?app_id={config.api_onesig}"

    payload = {
        "extra_fields": ["notification_types"]
    }

    headers = {
        "Accept": "application/json",
        "Authorization": "Basic NWVjZGM3ZDUtYzM5Yi00MmUxLTk4MjAtNTJlNDAwMTAwOWJj",
        "Content-Type": "application/json",
        "Host": "api.onesignal.com",
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        print(f"Failed to trigger export. Status code: {response.status_code}")
        return

    response_dict = json.loads(response.text)
    csv_file_url = response_dict['csv_file_url']

    # Step 2: Wait for 1 minute with a countdown before attempting to download the file
    wait_time = 15
    print("Export en cours. Patientez avant téléchargement...")

    for remaining in range(wait_time, 0, -1):
        if remaining > 10 and remaining % 10 == 0:
            print(f"Temps restant : {remaining} sec...")
        elif remaining <= 10:
            print(f"{remaining}")
        time.sleep(1)

    print("\nTentative de téléchargement...")

    # Step 3: Attempt to download the file
    download_response = requests.get(csv_file_url, stream=True)

    if download_response.status_code != 200:
        print(f"Echec. Status code: {download_response.status_code}")
        return

    # Step 4: Save the file
    gzip_file_name = "/Users/patrick/PycharmProjects/one/csv/todo/onesig_base.csv.gz"
    with open(gzip_file_name, 'wb') as file:
        shutil.copyfileobj(download_response.raw, file)

    # Step 5: Decompress the file
    decompressed_file_name = gzip_file_name[:-3]  # Remove the .gz extension
    with gzip.open(gzip_file_name, 'rb') as f_in:
        with open(decompressed_file_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


    # Step 6: Count the number of rows in the CSV file
    df = pd.read_csv(decompressed_file_name)
    import numpy as np

    # Assuming 'df' is your DataFrame
    df['notification_types'] = df['notification_types'].replace({-22: 'No', -2: 'No'})

    # Replace empty values (NaN) with 'Yes'
    df['notification_types'] = df['notification_types'].replace(np.nan, 'Yes')

    # If there are empty strings instead of NaN, you can use:
    df['notification_types'] = df['notification_types'].replace('', 'Yes')

    df.to_csv("/Users/patrick/PycharmProjects/one/csv/todo/onesig_base.csv")


    row_count = len(df)
    # Optionally, remove the .gz file after decompression
    os.remove(gzip_file_name)


    print(f"Fichier téléchargé, il y a {row_count} lignes.")
    # Split de la colonne tags
    df['tags'] = df['tags'].apply(json.loads)
    tags_df = pd.json_normalize(df['tags'])
    df = df.drop(columns=['tags']).join(tags_df, lsuffix='_left', rsuffix='_right')

    # Sauvegarder le DataFrame df1 après avoir splité les tags

    df.to_csv("/Users/patrick/PycharmProjects/one/csv/results/onesig_tags_splited.csv", index=False)

def envoi_email(status,error):
    SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/gmail.readonly']

    sender_email = 'ope@supertripper.com'
    sender_name = 'Supertripper Reports'
    recipient_email = "ope@supertripper.com"
    subject = f'CRON "One Signal" {status}'

    # Construction du corps de l'e-mail
    body = (
        f'{error}'
    )
    creds_file = 'creds/cred_gmail.json'
    token_file = 'token.json'
    def authenticate_gmail():
        """Authentifie l'utilisateur via OAuth 2.0 et retourne les credentials"""
        creds = None
        # Le token est stocké localement après la première authentification
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        # Si le token n'existe pas ou est expiré, on initie un nouveau flux OAuth
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
                creds = flow.run_local_server(port=0)
            # Enregistrer le token pour des sessions futures
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        return creds

    def create_message_with_attachment(sender, sender_name, to, subject, message_text):
        """Crée un e-mail avec une pièce jointe et un champ Cc"""
        message = MIMEMultipart()
        message['to'] = to
        message['from'] = f'{sender_name} <{sender}>'
        message['subject'] = subject

        # Attacher le corps du texte
        message.attach(MIMEText(message_text, 'plain'))

        # Encoder le message en base64 pour l'envoi via l'API Gmail
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        return {'raw': raw_message}

    def send_email(service, user_id, message):
        """Envoie un e-mail via l'API Gmail"""
        try:
            message = service.users().messages().send(userId=user_id, body=message).execute()
            print(f"Message Id: {message['id']}")
            return message
        except HttpError as error:
            print(f'An error occurred: {error}')
            return None

    # Authentifier l'utilisateur et créer un service Gmail
    creds = authenticate_gmail()
    service = build('gmail', 'v1', credentials=creds)

    # Créer le message avec pièce jointe et copie
    message = create_message_with_attachment(sender_email, sender_name, recipient_email, subject, body)

    # Envoyer l'e-mail
    send_email(service, 'me', message)
    print("Mail envoyé pour vérif ")








