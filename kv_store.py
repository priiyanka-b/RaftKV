class KVStore:
    def __init__(self):
        self.store={}
                            
    def apply(self,command):
        if command["op"]=="put":
            self.store[command["key"]]=command["value"]
        elif command["op"]=="get":
            if command["key"] in self.store:
                return self.store[command["key"]]
            else:
                return "Key Not Found"
        elif command["op"]=="delete":
            if command["key"] in self.store:
                del self.store[command["key"]]
            else:
                return "Key Not Found"


