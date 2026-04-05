SELECT p.passenger_id
FROM Passenger p
LEFT JOIN Reserve r ON p.passenger_id = r.passenger_id
WHERE r.seat IS NULL;