import random
import time

class DataStore:
    def __init__(self):
        self._data = {}
        self.memory_usage=0

    def set(self, key, value,expiry_time=None):
        if key in self._data:
            #key:(value,data_type, expiry_time)
            old_value,_,_=self._data[key]
            self.memory_usage -= self._calculate_memory_usage(key,old_value)

        data_type=self._get_data_type(value)
        self._data[key]=(value,data_type,expiry_time)
        self.memory_usage +=self._calculate_memory_usage(key,value)

        

    def get(self, key):
        if not self._is_key_valid(key):
            return None
        value,_,_= self._data[key]
        return value 
    

    def delete(self, *keys):
        count = 0
        for key in keys:
            if key in self._data:
                value,_,_=self._data[key]
                self.memory_usage -=self._calculate_memory_usage(key,value)
                del self._data[key]
                count += 1
        return count

    def exists(self, *keys):
        return sum(1 for key in keys if self._is_key_valid(key))
    
    

    def keys(self):
        return list(self._data.keys())

    def flush(self):
        self._data.clear()
        self.memory_usage=0

    def expire(self,key,seconds):
        """Set expiration time in seconds from now"""
        if not self._is_key_valid(key):
            return False
        
        value,data_type,_=self._data[key]
        expiry_time= time.time() + seconds
        self._data[key]=(value,data_type,expiry_time)
        return True


    def expire_at(self, key, timestamp):
        """Set expiration to a specific Unix timestamp"""
        if not self._is_key_valid(key):
            return False
    
        # Unpack all 3 items to match your dictionary structure
        value, data_type, _ = self._data[key]
    
        # Set the expiry directly to the provided timestamp
        self._data[key] = (value, data_type,timestamp)
        return True
    
    def ttl(self,key):
        # Lazy expiration
        # access the value
        # return the remaining time to live

        if not self._is_key_valid(key):
            return -2  # Redis standard: -2 means key does not exist
        _,_,expiry_time=self._data[key]
        if expiry_time is None:
            return -1  # Redis standard: -1 means key exists but has no expiry
        
        # Lazy Expiration Check
        current_time=time.time()
        if expiry_time <=current_time:
            self.delete(key)
            return -2
        
        # Return the remaining time (rounded to seconds)
        return int(expiry_time - time.time())
    

    
    def pttl(self, key):
        """Return the remaining time to live in milliseconds"""
        if not self._is_key_valid(key):
            return -2
            
        _, _, expiry_time = self._data[key]
        
        if expiry_time is None:
            return -1
            
        current_time = time.time()
        
        # Check for expiration (Lazy deletion)
        if expiry_time <= current_time:
            self.delete(key)
            return -2
            
        # Multiply by 1000 to convert seconds to milliseconds
        # We use int() because Redis PTTL returns an integer
        return int((expiry_time - current_time) * 1000)
    

    
    def persist(self,key):
        """Remove the expiration from a key, making it persistent"""
        if not self._is_key_valid(key):
            return 0 # Redis returns 0 if the key does not exist
        
        value, data_type, expiry_time = self._data[key]
        if expiry_time is None:
            return 0  # Redis returns 0 if the key was already persistent
        
        self._data[key]=(value, data_type, None)
        return 1
    
    def get_type(self,key):
        """Return the redis data type of the value stored at key"""
        if not self._is_key_valid(key):
            return "none" 
        _,data_type,_=self._data[key]
        return data_type
    

            

    def _calculate_memory_usage(self, key, value):
    # Convert to string, then encode to bytes, then get the length
        key_bytes = str(key).encode('utf-8')
        value_bytes = str(value).encode('utf-8')
    
        key_size = len(key_bytes)
        value_size = len(value_bytes)
    
        return key_size + value_size + 64 # Overhead for metadata
    

    def _get_data_type(self,value):
        """Determine the Redis data type based on the python value"""
        if isinstance(value, str):
            return "string"
        elif isinstance(value,int):
            return "string" # redis treats numbers as a string type
        elif isinstance(value, list):
            return "list"
        elif isinstance(value, set):
            return "set"
        elif isinstance(value, dict):
            return "hash"
        else:
            return "string" #default to string 
        
    
    #Lazy Expiration
    #we check if a key is expired only when we acccess it.
    def _is_key_valid(self,key):
        """Lazy expiration"""
        if key not in self._data:
            return False
        
        value,_,expiry_time=self._data[key]
        if expiry_time is not None and expiry_time <= time.time():
            #key expired
            self.delete(key)
            return False
        
        return True

        
    def cleanup_expired_keys(self):
        """Background cleanup of expired keys"""
        if not self._data:
            return 0
        
        current_time=time.time()
        expired_keys=[]

        MAX_SAMPLE_SIZE=20

        sample_size= min(20, len(self._data))

        sample_keys= random.sample(list(self._data.keys()),sample_size)
        

        for key in sample_keys:
            if key not in self._data:
                continue
            value,_,expiry_time=self._data[key]
            if expiry_time is not None and expiry_time <= current_time:
                expired_keys.append(key)

        for key in expired_keys:
            value,_,_=self._data[key]
            self.memory_usage -=self._calculate_memory_usage(key,value)
            del self._data[key]

        return len(expired_keys)
    
