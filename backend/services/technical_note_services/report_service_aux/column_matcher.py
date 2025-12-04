from typing import Dict, Any, List, Optional, Set
from utils.text_normalizer import normalize_text


class ColumnMatcher:
    """Responsable de encontrar y filtrar mappings de columnas"""
    
    @staticmethod
    def find_matching_columns(
        all_columns: List[str],
        column_mappings: Optional[Dict[str, Any]],
        keywords: Optional[List[str]]
    ) -> List[str]:
        """Encuentra columnas del dataset que coinciden con los mappings"""
        
        matching_columns_set: Set[str] = set()
        
        if not column_mappings or 'mappings' not in column_mappings:
            return []
        
        print("Analizando mappings...")
        relevant_mappings = ColumnMatcher._filter_relevant_mappings(
            column_mappings, keywords
        )
        
        print(f"   {len(relevant_mappings)} mappings relevantes para keywords {keywords}")
        
        mapped_columns = ColumnMatcher._extract_mapped_columns(relevant_mappings)
        print(f"   {len(mapped_columns)} columnas únicas en mappings")
        
        for col_dataset in all_columns:
            col_dataset_norm = normalize_text(col_dataset)
            for mapped_col in mapped_columns:
                mapped_col_norm = normalize_text(mapped_col)
                if col_dataset_norm == mapped_col_norm:
                    matching_columns_set.add(col_dataset)
                    print(f"   MATCH: Dataset '{col_dataset}' <- Mapping '{mapped_col}'")
                    break
        
        return list(matching_columns_set)
    
    @staticmethod
    def _filter_relevant_mappings(
        column_mappings: Dict[str, Any],
        keywords: Optional[List[str]]
    ) -> List[Dict[str, Any]]:
        """Filtra mappings relevantes según keywords"""
        
        relevant_mappings = []
        
        for mapping in column_mappings.get('mappings', []):
            columna_usuario = mapping.get('columna_usuario', '')
            mapping_keyword = mapping.get('keyword', '')
            
            if not columna_usuario or not mapping_keyword:
                continue
            
            if keywords:
                keywords_norm = [normalize_text(kw) for kw in keywords]
                mapping_keyword_norm = normalize_text(mapping_keyword)
                
                for kw_norm in keywords_norm:
                    if kw_norm in mapping_keyword_norm or mapping_keyword_norm in kw_norm:
                        relevant_mappings.append(mapping)
                        if len(relevant_mappings) <= 3:
                            print(f"   Mapping incluido: '{columna_usuario}' (keyword: '{kw_norm}')")
                        break
            else:
                relevant_mappings.append(mapping)
        
        return relevant_mappings
    
    @staticmethod
    def _extract_mapped_columns(mappings: List[Dict[str, Any]]) -> Set[str]:
        """Extrae columnas únicas de los mappings"""
        
        mapped_columns = set()
        for mapping in mappings:
            columna_usuario = mapping.get('columna_usuario', '')
            if columna_usuario:
                mapped_columns.add(columna_usuario)
        
        return mapped_columns
    
    @staticmethod
    def find_all_mappings_for_column(
        column_name: str,
        column_mappings: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Encuentra TODOS los mappings para una columna específica"""
        
        if not column_mappings:
            return []
        
        all_mappings = []
        col_norm = normalize_text(column_name)
        
        print(f"      Buscando mappings para columna: '{column_name}'")
        print(f"         Normalizada: '{col_norm}'")
        
        for idx, mapping in enumerate(column_mappings.get("mappings", [])):
            mapping_col = mapping.get("columna_usuario", "")
            mapping_col_norm = normalize_text(mapping_col)
            
            if col_norm == mapping_col_norm:
                consolidado = mapping.get('consolidado', {})
                edad_aplicable = consolidado.get('edad_aplicable', '')
                consulta_proc = consolidado.get('consulta_procedimiento', '')
                
                all_mappings.append(mapping)
                print(f"         [{len(all_mappings)}] Match #{idx}: Edad '{edad_aplicable}', Consulta '{consulta_proc[:50]}...'")
        
        print(f"      Total mappings encontrados: {len(all_mappings)}")
        
        return ColumnMatcher._deduplicate_mappings(all_mappings)
    
    @staticmethod
    def _deduplicate_mappings(mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Elimina mappings duplicados basándose en edad y consulta"""
        
        unique_mappings = []
        seen_keys = set()
        
        for mapping in mappings:
            consolidado = mapping.get('consolidado', {})
            edad_aplicable = consolidado.get('edad_aplicable', '')
            consulta_proc = consolidado.get('consulta_procedimiento', '')
            
            key = (normalize_text(edad_aplicable), normalize_text(consulta_proc))
            
            if key not in seen_keys:
                unique_mappings.append(mapping)
                seen_keys.add(key)
            else:
                print(f"      Duplicado omitido: Edad '{edad_aplicable}'")
        
        print(f"      Mappings únicos después de deduplicar: {len(unique_mappings)}")
        
        return unique_mappings
