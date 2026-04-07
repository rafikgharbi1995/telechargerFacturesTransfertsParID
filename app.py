import streamlit as st
from azure.storage.blob import BlobServiceClient
import os
import pandas as pd
from io import StringIO

# ==================== CONFIGURATION ====================
st.set_page_config(
    page_title="Azure Blob Downloader",
    page_icon="📁",
    layout="wide"
)

st.title("📁 Téléchargement automatique - Factures & Transferts")
st.markdown("Recherche et enregistre les fichiers CSV directement sur votre disque")


# ==================== FONCTIONS ====================
def read_ids_from_text(text):
    ids = []
    for line in text.splitlines():
        clean_line = line.strip()
        if not clean_line or clean_line.startswith('#'):
            continue
        clean_id = clean_line.strip("'\" ,;")
        if clean_id:
            ids.append(clean_id)
    return ids


def download_file(connection_string, container_name, blob_path, download_folder):
    """Télécharge et enregistre physiquement le fichier dans download_folder"""
    try:
        os.makedirs(download_folder, exist_ok=True)
        filename = os.path.basename(blob_path)
        local_path = os.path.join(download_folder, filename)

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        with open(local_path, "wb") as f:
            f.write(blob_client.download_blob().readall())
        return local_path
    except Exception as e:
        st.error(f"Erreur téléchargement {blob_path}: {e}")
        return None


def search_and_download(connection_string, container_name, id_list, prefixes, download_folder, progress_bar,
                        status_text):
    id_set = set(id_list)
    results = {search_id: {'found': False, 'files': []} for search_id in id_set}
    downloaded_files = []

    os.makedirs(download_folder, exist_ok=True)

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        all_blobs = []
        for prefix in prefixes:
            blobs = list(container_client.list_blobs(name_starts_with=prefix))
            all_blobs.extend(blobs)

        total_files = len(all_blobs)
        status_text.text("🔍 Recherche des fichiers...")
        progress_bar.progress(0)

        for idx, blob in enumerate(all_blobs):
            blob_name = blob.name
            if not blob_name.lower().endswith('.csv'):
                continue
            for search_id in id_set:
                if search_id in blob_name:
                    if blob_name not in results[search_id]['files']:
                        results[search_id]['found'] = True
                        results[search_id]['files'].append(blob_name)
            if total_files > 0:
                progress_bar.progress(int((idx + 1) / total_files * 50))

        status_text.text("📥 Enregistrement des fichiers...")
        ids_with_files = [sid for sid in id_set if results[sid]['found']]
        for idx, search_id in enumerate(ids_with_files):
            for file_path in results[search_id]['files']:
                local_path = download_file(connection_string, container_name, file_path, download_folder)
                if local_path:
                    downloaded_files.append({
                        'id': search_id,
                        'azure_path': file_path,
                        'local_path': local_path
                    })
            if len(ids_with_files) > 0:
                progress_bar.progress(50 + int((idx + 1) / len(ids_with_files) * 50))

        progress_bar.progress(100)
        status_text.text("✅ Terminé !")
        return results, downloaded_files, len(all_blobs), sum(len(results[sid]['files']) for sid in id_set)
    except Exception as e:
        st.error(f"Erreur : {e}")
        return {}, [], 0, 0


# ==================== INTERFACE ====================
with st.sidebar:
    st.header("⚙️ Configuration")

    # Lecture de la clé depuis variable d'environnement (optionnelle)
    default_conn_string = os.environ.get("AZURE_CONNECTION_STRING", "")

    connection_string = st.text_input(
        "Chaîne de connexion Azure",
        value=default_conn_string,  # plus de clé en dur !
        type="password"
    )
    container_name = st.text_input("Nom du conteneur", value="archive")

    st.divider()
    st.header("📂 Dossiers à parcourir")
    search_transfers = st.checkbox("transfers/", value=True)
    search_invoices = st.checkbox("invoices/", value=True)

    st.divider()
    st.header("💾 Dossier de destination (sur votre disque)")
    download_folder = st.text_input(
        "Chemin local où les fichiers seront sauvegardés",
        value=r"C:\Users\helpdesk10\Desktop\RO\TransfertManquantsLCW\Exist_Files"
    )

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("🔎 IDs à rechercher")
    input_method = st.radio("Méthode de saisie", ["Saisie manuelle", "Importer un fichier .txt"], horizontal=True)

    ids = []
    if input_method == "Saisie manuelle":
        text_ids = st.text_area("Entrez les IDs (un par ligne)", height=200)
        if text_ids:
            ids = read_ids_from_text(text_ids)
    else:
        uploaded_file = st.file_uploader("Choisir un fichier texte", type=["txt"])
        if uploaded_file:
            content = StringIO(uploaded_file.getvalue().decode("utf-8")).read()
            ids = read_ids_from_text(content)
            st.success(f"{len(ids)} ID(s) trouvé(s)")

with col2:
    if st.button("🚀 Lancer la recherche et l'enregistrement", type="primary", use_container_width=True):
        if not connection_string:
            st.error("Veuillez entrer la chaîne de connexion Azure")
        elif not ids:
            st.error("Veuillez fournir au moins un ID")
        else:
            prefixes = []
            if search_transfers:
                prefixes.append("transfers/")
            if search_invoices:
                prefixes.append("invoices/")
            if not prefixes:
                st.error("Sélectionnez au moins un dossier")
            else:
                # Vérifier que le dossier de destination est accessible
                try:
                    os.makedirs(download_folder, exist_ok=True)
                except Exception as e:
                    st.error(f"Impossible d'écrire dans {download_folder}: {e}")
                    st.stop()

                progress_bar = st.progress(0)
                status_text = st.empty()

                results, downloaded_files, total_files, found_count = search_and_download(
                    connection_string, container_name, ids, prefixes, download_folder, progress_bar, status_text
                )

                st.subheader("📈 Résultats")
                col_r1, col_r2, col_r3 = st.columns(3)
                col_r1.metric("Fichiers analysés", total_files)
                col_r2.metric("Correspondances trouvées", found_count)
                col_r3.metric("Fichiers enregistrés", len(downloaded_files))

                if downloaded_files:
                    df = pd.DataFrame([{
                        "ID": f["id"],
                        "Fichier": os.path.basename(f["azure_path"]),
                        "Enregistré dans": f["local_path"]
                    } for f in downloaded_files])
                    st.dataframe(df, use_container_width=True)
                    st.success(f"✅ {len(downloaded_files)} fichier(s) enregistré(s) dans : **{download_folder}**")
                else:
                    st.warning("Aucun fichier trouvé pour les IDs demandés.")

                not_found = [id for id, data in results.items() if not data['found']]
                if not_found:
                    st.warning(f"⚠️ IDs non trouvés : {', '.join(not_found)}")

                st.success("Opération terminée !")

st.divider()
st.caption("Les fichiers sont directement sauvegardés sur votre disque au chemin indiqué.")