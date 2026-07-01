import re
from pathlib import Path
from typing import Dict, Any, List

class AdvancedDeveloperTools:
    
    @staticmethod
    def workspace_search(pattern: str, extension: str = "*.py") -> List[Dict[str, Any]]:
        """Acts like a local 'grep' engine to locate strings or patterns across files without reading them entirely into context."""
        matches = []
        cwd = Path(".")
        
        for path in cwd.rglob(extension):
            if ".venv" in path.parts or "__pycache__" in path.parts:
                continue
            try:
                content = path.read_text()
                for i, line in enumerate(content.splitlines(), 1):
                    if re.search(pattern, line, re.IGNORECASE):
                        matches.append({
                            "file": str(path),
                            "line": i,
                            "matched_text": line.strip()
                        })
            except Exception:
                continue
        return matches

    @staticmethod
    def surgical_patch(filename: str, search_block: str, replace_block: str) -> str:
        """Locates a precise block of text inside a large file and swaps it out surgically."""
        target_path = Path(filename)
        if not target_path.exists():
            raise FileNotFoundError(f"Target workspace file '{filename}' does not exist.")
            
        content = target_path.read_text()
        
        # Normalize line endings to prevent OS-specific string matching failures
        search_block_norm = search_block.strip().replace("\r\n", "\n")
        replace_block_norm = replace_block.strip().replace("\r\n", "\n")
        content_norm = content.replace("\r\n", "\n")
        
        if search_block_norm not in content_norm:
            raise ValueError("The target search_block could not be found with exact string matching inside the source file.")
            
        updated_content = content_norm.replace(search_block_norm, replace_block_norm)
        target_path.write_text(updated_content)
        return f"Surgical update successful on {filename}."