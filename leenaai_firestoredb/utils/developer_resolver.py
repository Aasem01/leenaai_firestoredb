from typing import List
from difflib import get_close_matches

from src.utils.user_language_normalization import LanguageNormalizer


class DeveloperNameResolver:
    """
    Resolves developer names using three-tier search strategy:
    1. Exact match in Arabic or English
    2. Fuzzy match if no exact match found
    3. Partial match if no fuzzy match found
    """
    
    @staticmethod
    def filter_developers_by_name(developers: List[dict], developer_name: str) -> List[dict]:
        """
        Filter developers by name using three-tier search:
        1. Exact match in Arabic or English
        2. Fuzzy match if no exact match found
        3. Partial match if no fuzzy match found
        """
        normalized_search_name = LanguageNormalizer.shared().normalize_text(developer_name)
        
        # Tier 1: Exact match in Arabic or English
        exact_matches = [
            developer 
            for developer in developers
            if (LanguageNormalizer.shared().normalize_text(developer.get("en_name", "")) == normalized_search_name or
                LanguageNormalizer.shared().normalize_text(developer.get("ar_name", "")) == normalized_search_name)
        ]
        
        if exact_matches:
            return exact_matches
        
        # Tier 2: Fuzzy match
        fuzzy_matches = []
        for developer in developers:
            en_name = LanguageNormalizer.shared().normalize_text(developer.get("en_name", ""))
            ar_name = LanguageNormalizer.shared().normalize_text(developer.get("ar_name", ""))
            
            if get_close_matches(normalized_search_name, [en_name, ar_name], n=1, cutoff=0.95):
                fuzzy_matches.append(developer)
        
        if fuzzy_matches:
            return fuzzy_matches
        
        # Tier 3: Partial match (search name is part of developer name)
        partial_matches = [
            developer 
            for developer in developers
            if (normalized_search_name in LanguageNormalizer.shared().normalize_text(developer.get("en_name", "")) or
                normalized_search_name in LanguageNormalizer.shared().normalize_text(developer.get("ar_name", "")))
        ]
        
        return partial_matches 