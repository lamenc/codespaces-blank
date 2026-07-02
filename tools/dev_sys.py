
import re
from pathlib import Path
from typing import List, Dict, Any

class AdvancedDeveloperTools:
    @staticmethod
    def workspace_search(pattern: str, extension: str = '*.py') -> List[Dict[str, Any]]:
        """
        Use pathlib to recursively scan the workspace for a regex pattern in files,
        returning a list of dictionaries with {'file': path, 'line': line_number, 'matched_text': line_content}.
        """
        results = []
        for file_path in Path('/workspaces/codespaces-blank').rglob(extension):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_number, line_content in enumerate(f, 1):
                        if re.search(pattern, line_content):
                            results.append({
                                'file': str(file_path),
                                'line': line_number,
                                'matched_text': line_content.strip()
                            })
            except UnicodeDecodeError:
                # Skip files that cannot be decoded with utf-8
                continue
        return results

    @staticmethod
    def surgical_patch(filename: str, search_block: str, replace_block: str):
        """
        Read the file, perform an exact string replacement of search_block with replace_block, and overwrite the file.
        Raise FileNotFoundError or ValueError if the file or block is missing.
        """
        file_path = Path(filename)
        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {filename}")

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        if search_block not in content:
            raise ValueError(f"Search block not found in file: {filename}")

        updated_content = content.replace(search_block, replace_block, 1)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)

