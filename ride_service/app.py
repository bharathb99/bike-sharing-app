import pika
import json
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user:password@ride_db:5432/ridedb'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class Ride(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    origin = db.Column(db.String(100), nullable=False)
    destination = db.Column(db.String(100), nullable=False)
    seats = db.Column(db.Integer, nullable=False)

    def to_dict(self):
        return {"id": self.id, "origin": self.origin, "destination": self.destination, "seats": self.seats}

# Define the association table AFTER the models are defined
user_rides = db.Table('user_rides',
    db.Column('user_id', db.Integer, db.ForeignKey('user.id'), primary_key=True),
    db.Column('ride_id', db.Integer, db.ForeignKey('ride.id'), primary_key=True)
)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

Ride.users = db.relationship('User', secondary=user_rides, backref='rides')

def publish_message(message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', 5672))  # Connect to AMQP port
        channel = connection.channel()
        channel.queue_declare(queue='notifications', durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key='notifications',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        connection.close()
    except Exception as e:
        app.logger.error(f"Failed to publish message: {e}")

@app.route('/rides', methods=['POST'])
def create_ride():
    data = request.json
    new_ride = Ride(origin=data['origin'], destination=data['destination'], seats=data['seats'])
    db.session.add(new_ride)
    db.session.commit()

    # Send notification
    publish_message({
        "type": "ride_created",
        "origin": new_ride.origin,
        "destination": new_ride.destination
    })
    
    return jsonify({"message": "Ride created successfully!", "ride": new_ride.to_dict()}), 201

@app.route('/rides/<int:ride_id>/join', methods=['POST'])
def join_ride(ride_id):
    data = request.json
    user_id = data['user_id']
    user = User.query.get(user_id)
    ride = Ride.query.get(ride_id)
    
    if ride and user:
        ride.users.append(user)
        db.session.commit()

        # Send notification
        publish_message({
            "type": "ride_joined",
            "user": user.username,
            "ride": ride.origin + " to " + ride.destination
        })
        return jsonify({"message": "User joined the ride!"}), 200
    return jsonify({"message": "Ride or user not found!"}), 404

@app.route('/rides/<int:ride_id>/leave', methods=['POST'])
def leave_ride(ride_id):
    data = request.json
    user_id = data['user_id']
    user = User.query.get(user_id)
    ride = Ride.query.get(ride_id)
    
    if ride and user:
        ride.users.remove(user)
        db.session.commit()

        # Send notification
        publish_message({
            "type": "ride_left",
            "user": user.username,
            "ride": ride.origin + " to " + ride.destination
        })
        return jsonify({"message": "User left the ride!"}), 200
    return jsonify({"message": "Ride or user not found!"}), 404

# Get all rides
@app.route('/rides', methods=['GET'])
def get_all_rides():
    rides = Ride.query.all()
    return jsonify([ride.to_dict() for ride in rides]), 200

# Get ride by ID
@app.route('/rides/<int:ride_id>', methods=['GET'])
def get_ride(ride_id):
    ride = Ride.query.get(ride_id)
    if ride:
        return jsonify(ride.to_dict()), 200
    return jsonify({"message": "Ride not found!"}), 404

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=5001)
