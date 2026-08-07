import warnings
import os
from utils import load_env
import requests
from urllib3.exceptions import InsecureRequestWarning




"""
POIRES signifie Page de l’Outil d’Interface de REconciliations Serveur

Ces fonctions permettent d'exécuter une réconciliation recserveur via le formulaire POIRES
"""

def _get_poires_params() -> dict:

    load_env()

    return {
        "url": os.getenv("POIRES_URL"),
        "login": os.getenv("POIRES_LOGIN"),
        "division": os.getenv("POIRES_DIVISION"),
        "pom_referer_url": os.getenv("POIRES_REFERER_URL"),
    }

def _get_conn_bduni_params() -> dict:

    load_env()

    return {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "user": os.getenv("POSTGRES_USER_PBM"),
        "password": os.getenv("POSTGRES_PASSWORD_PBM"),
        "database": os.getenv("POSTGRES_DB"),
    }



def launch_poires(
    table_name: str | list[str] = "insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage",
    change_comment: str = "",
    zr_name: str = "Reconcilation RNB diff",
    nature_operation: str = "Documentaire",
    timeout: int = 300,
):
    # conf_poires = self.config.get("poires", out_type=dict)
    # conf_bduni = self.config.get("db.bduni_recserveur", out_type=dict)
    conf_poires = _get_poires_params()
    conf_bduni = _get_conn_bduni_params()

    # imitation d'un navigateur
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
        "Referer": conf_poires["pom_referer_url"],
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }

    post_payload = {
        "login": conf_poires["login"],
        "division": conf_poires["division"],
        "host": conf_bduni["host"],
        "port": conf_bduni["port"],
        "bd": conf_bduni["database"],
        "schema": "recserveur",
        "sel_table[]": table_name,
        "forecezipva": "non",  # pas de réconcialiation sur les zones interdites
        "zr": zr_name,
        "changement": change_comment,
        "no": nature_operation,
        "m": "oui",
        "source_1": "",
        "date_des_donnees": "",
    }

    # post_payload = {
    #     "login": "ali_bot",
    #     "division": "OVT",
    #     "host": "bduni_preprod.ign.fr",
    #     "port": "5432",
    #     "bd": "bduni_preprod",
    #     "schema": "recserveur",
    #     "sel_table[]": "insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage",
    #     "forecezipva": "non",  # pas de réconcialiation sur les zones interdites
    #     "zr": "Batiment_RNB_nouveaux",
    #     "changement": "",
    #     "no": nature_operation,
    #     "m": "oui",
    #     "source_1": "",
    #     "date_des_donnees": "",
    # }

    with warnings.catch_warnings(action="ignore", category=InsecureRequestWarning):
        r = requests.post(
            conf_poires["url"],
            headers=headers,
            data=post_payload,
            timeout=timeout,
            verify=False,
        )

    response = r.json()

    if response["attention"] != "":
        print(response["attention"])

    if response["err"] != "":
        print("erreur : %s", response)
       
    if response["erreur_recserveur"] != "":
        print("erreur_recserveur : %s", response["erreur_recserveur"])
        
    print(response["mess_recserveur"])
    print(response["mess_bilan"])

    if response["surzipva"] != "":
        print(response["surzipva"])

    print("numrec : ",response["numrec"])

    return response

def launch_poires_DATAC(
    # self,
    # table_name: str | list[str],
    #change_comment: str,
    #zr_name: str = "Reconcilation RNB diff",
    nature_operation: str = "Documentaire",
    timeout: int = 300,
):
    # conf_poires = self.config.get("poires", out_type=dict)
    # conf_bduni = self.config.get("db.bduni_recserveur", out_type=dict)

    # imitation d'un navigateur
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
        "Referer": "https://pomme.ign.fr/BDUni/Outils/Poires/",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }

   
    post_payload = {
        "login": "ali_bot",
        "operateur": "",
        "division": "OVT",
        "choix-serveur": "preprod",
        "sel_table[]": "insert_batiment_rnb_lien_bdtopo__batiments_rnb_moissonnage",
        "forecezipva": "non",  # pas de réconcialiation sur les zones interdites
        "zr": "Batiment_RNB_nouveaux",
        "changement": "",
        "no": nature_operation,
        "source": "",
        "connexion_ini": "pbm",
        "appel_de": "Poires automatique",
        "sujet-email": "[Poires auto] reconciliation Moissonnage RNB",
        "obj_plantage":"",
    }

    with warnings.catch_warnings(action="ignore", category=InsecureRequestWarning):
        r = requests.post(
            "https://pomme.ign.fr/Services-Web/Poires-DATAC/reconcilier.ajax.php?effacer_ensuite",
            headers=headers,
            data=post_payload,
            timeout=timeout,
            verify=False,
        )

    print("Status:", r.status_code)

    print("Content-Type:", r.headers.get("Content-Type"))

    print("Response:")

    print(repr(r.text))
    response = r.json()
    print(response)

    # if response["attention"] != "":
    #     print(response["attention"])

    # if response["err"] != "":
    #     print("erreur : %s", response)
       
    # if response["erreur_recserveur"] != "":
    #     print("erreur_recserveur : %s", response["erreur_recserveur"])
        
    # print(response["mess_recserveur"])
    # print(response["mess_bilan"])

    # if response["surzipva"] != "":
    #     print(response["surzipva"])

    # print("numrec : ",response["numrec"])

    return response


# test execution launch
launch_poires()
# launch_poires_DATAC()