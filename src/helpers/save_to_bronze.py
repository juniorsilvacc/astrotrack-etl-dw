import os
import json
import logging

def save_to_bronze(data: dict, api_name: str, suffix: str):
    """Função genérica para salvar qualquer JSON na camada Bronze."""
    base_path = f"data/bronze/{suffix}"
    os.makedirs(base_path, exist_ok=True)
    
    file_name = f"{api_name}_{suffix}_raw.json"
    full_path = os.path.join(base_path, file_name)
    
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    logging.info(f"Dados de {suffix} salvos em: {full_path} ✅")
    return full_path