import random
import uuid
from datetime import datetime


games = ["football", "basketball", "tennis", "hockey", "esports"]
statuses = ["win", "lose", "pending"]

def create_event():
    return {
        "bet_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()), 
        "game": random.choice(games),
        "amount": round(random.uniform(5, 500), 2),
        "status": random.choice(statuses),
        "timestamp": datetime.utcnow().isoformat()
    }

def main():
    
    event = create_event()
    print(event)

if __name__ == "__main__":
    main()
