import json
from datetime import datetime
from typing import Dict, List, Any, Union

class DataUnifier:
    """
    Algorithm to unify two different data models with different timestamp formats.
    Converts ISO format timestamps to milliseconds and unifies the data structure.
    """
    
    def __init__(self):
        self.unified_data = []
    
    def iso_to_milliseconds(self, iso_timestamp: str) -> int:
        """
        Convert ISO timestamp to milliseconds since epoch.
        
        Args:
            iso_timestamp (str): ISO format timestamp (e.g., "2023-10-15T14:30:45.123Z")
            
        Returns:
            int: Timestamp in milliseconds since epoch
        """
        # IMPLEMENT: Parse ISO timestamp and convert to milliseconds
        try:
            # Remove 'Z' if present and parse the timestamp
            if iso_timestamp.endswith('Z'):
                iso_timestamp = iso_timestamp[:-1]
            
            # Parse the ISO timestamp
            dt = datetime.fromisoformat(iso_timestamp)
            
            # Convert to milliseconds since epoch
            milliseconds = int(dt.timestamp() * 1000)
            return milliseconds
            
        except ValueError as e:
            raise ValueError(f"Invalid ISO timestamp format: {iso_timestamp}") from e
    
    def normalize_data_format_1(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize data from format 1 (assuming it already has millisecond timestamps).
        
        Args:
            data (Dict): Data in format 1
            
        Returns:
            Dict: Normalized data structure
        """
        # IMPLEMENT: Extract and normalize data from format 1
        # Customize these field mappings based on your actual data-1.json structure
        normalized = {
            "timestamp": data.get("timestamp"),  # Already in milliseconds
            "message": data.get("message", ""),
            "value": data.get("value", 0),
            "source": "format_1",
            "metadata": data.get("metadata", {})
        }
        return normalized
    
    def normalize_data_format_2(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize data from format 2 (assuming it has ISO timestamps).
        
        Args:
            data (Dict): Data in format 2
            
        Returns:
            Dict: Normalized data structure
        """
        # IMPLEMENT: Extract and normalize data from format 2
        # Convert ISO timestamp to milliseconds
        iso_timestamp = data.get("timestamp", data.get("time", ""))
        millisecond_timestamp = self.iso_to_milliseconds(iso_timestamp)
        
        # Customize these field mappings based on your actual data-2.json structure
        normalized = {
            "timestamp": millisecond_timestamp,
            "message": data.get("message", data.get("msg", "")),
            "value": data.get("value", data.get("val", 0)),
            "source": "format_2",
            "metadata": data.get("metadata", data.get("meta", {}))
        }
        return normalized
    
    def detect_data_format(self, data: Dict[str, Any]) -> str:
        """
        Detect which data format is being used based on timestamp format.
        
        Args:
            data (Dict): Input data
            
        Returns:
            str: "format_1" or "format_2"
        """
        timestamp = data.get("timestamp", data.get("time", ""))
        
        # If timestamp is a number (milliseconds), it's format 1
        if isinstance(timestamp, (int, float)):
            return "format_1"
        
        # If timestamp is a string with ISO format, it's format 2
        if isinstance(timestamp, str) and ("T" in timestamp or "Z" in timestamp):
            return "format_2"
        
        # Default to format 1 if unclear
        return "format_1"
    
    def unify_data(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Main algorithm to unify data from different formats.
        
        Args:
            data_list (List[Dict]): List of data entries in various formats
            
        Returns:
            List[Dict]: Unified data in consistent format
        """
        unified_results = []
        
        for data_entry in data_list:
            try:
                # Detect the format
                format_type = self.detect_data_format(data_entry)
                
                # Normalize based on detected format
                if format_type == "format_1":
                    normalized = self.normalize_data_format_1(data_entry)
                else:
                    normalized = self.normalize_data_format_2(data_entry)
                
                unified_results.append(normalized)
                
            except Exception as e:
                print(f"Error processing data entry: {data_entry}")
                print(f"Error: {e}")
                continue
        
        # Sort by timestamp for consistent ordering
        unified_results.sort(key=lambda x: x["timestamp"])
        
        return unified_results
    
    def load_and_unify_from_files(self, file1_path: str, file2_path: str) -> List[Dict[str, Any]]:
        """
        Load data from two different files and unify them.
        
        Args:
            file1_path (str): Path to first data file
            file2_path (str): Path to second data file
            
        Returns:
            List[Dict]: Unified data from both files
        """
        all_data = []
        
        # Load data from file 1
        try:
            with open(file1_path, 'r') as f:
                data1 = json.load(f)
                if isinstance(data1, list):
                    all_data.extend(data1)
                else:
                    all_data.append(data1)
            print(f"Loaded {len(data1) if isinstance(data1, list) else 1} entries from {file1_path}")
        except FileNotFoundError:
            print(f"File not found: {file1_path}")
        except json.JSONDecodeError as e:
            print(f"JSON decode error in {file1_path}: {e}")
        
        # Load data from file 2
        try:
            with open(file2_path, 'r') as f:
                data2 = json.load(f)
                if isinstance(data2, list):
                    all_data.extend(data2)
                else:
                    all_data.append(data2)
            print(f"Loaded {len(data2) if isinstance(data2, list) else 1} entries from {file2_path}")
        except FileNotFoundError:
            print(f"File not found: {file2_path}")
        except json.JSONDecodeError as e:
            print(f"JSON decode error in {file2_path}: {e}")
        
        # Unify all loaded data
        return self.unify_data(all_data)
    
    def save_unified_data(self, unified_data: List[Dict[str, Any]], output_path: str):
        """
        Save unified data to a JSON file.
        
        Args:
            unified_data (List[Dict]): Unified data to save
            output_path (str): Output file path
        """
        try:
            with open(output_path, 'w') as f:
                json.dump(unified_data, f, indent=2)
            print(f"Unified data saved to: {output_path}")
            print(f"Total unified entries: {len(unified_data)}")
        except Exception as e:
            print(f"Error saving unified data: {e}")


def main():
    """
    Main function to demonstrate the data unification algorithm.
    This is where you implement your solution for the repl.it project.
    """
    print("Starting data unification process...")
    
    # Create unifier instance
    unifier = DataUnifier()
    
    # Load and unify data from the project files
    unified_result = unifier.load_and_unify_from_files("data-1.json", "data-2.json")
    
    # Save to result file
    unifier.save_unified_data(unified_result, "data-result.json")
    
    print("Data unification complete!")
    
    # Optional: Print a sample of the unified data for verification
    if unified_result:
        print("\nSample unified data (first entry):")
        print(json.dumps(unified_result[0], indent=2))


if __name__ == "__main__":
    main()