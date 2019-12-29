from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

# Table Creation
#CREATE TABLE Reservation(reservation_id int PRIMARY KEY, seans_id int, room int, seat_row varchar, seat_number int, occupied boolean);
#Create Table Seans(seans_id int Primary Key, film_name text, date text, room int, all_place_occupied boolean);
# Create Table Rooms(room int Primary Key, capacity int); #ulatwienie dziala tylko kidy sale maja ta sama ilosc miejsc w kazdym rzedzie
# room 2 razy nie jest potrzebne ale wydaje mi sie e dla wygody moze zostac

#Inserts
#Insert into Rooms(room, capacity, numberOfRows) values (1, 20, 4);
#Insert into Reservation(reservation_id,seans_id, room,seat_row,seat_number,occupied) values (1,1,1,'C',3,true);
#Insert into Seans(seans_id, film_name, date, room, all_place_occupied) values (1,'joker','20:00 : 3.12.2019',1,false);


def PrintSeanses():
    seanses = session.execute('SELECT seans_id, film_name, date,room  FROM Seans')
    for s in seanses:
        print ("Film: "+s.film_name +"  Date: " +s.date+"  Room: " +str(s.room))

def GetLastReservationID():
    latestentry=session.execute('SELECT Max(reservation_id)  FROM Reservation ')

   # print(" last "+str(latestentry[0])) #not most elegant...mona sprbowac loswac id zamaiast dawac kolejne
    return latestentry[0].system_max_reservation_id

def GetLastSeansID():
    latestentry=session.execute('SELECT Max(seans_id)  FROM Seans ')

    print(" last "+str(latestentry[0].system_max_seans_id)) #not most elegant...mona sprbowac loswac id zamaiast dawac kolejne
    return latestentry[0].system_max_seans_id


def RegisterSeans(filmname, newdate, roomnumber):
    seansid = GetLastSeansID()
    seansid = seansid + 1

    session.execute("""Insert into Seans(seans_id, film_name, date, room, all_place_occupied) 
    values (%(seans_id)s,%(film_name)s,%(date)s,%(room)s,%(all_place_occupied)s)""",
                    {'seans_id': seansid, 'film_name': filmname, 'date': newdate, 'room': roomnumber,
                     'all_place_occupied': False})

def PrintSeansSeatsWithReservations(selected_seans, selected_room):
    rooms = session.execute("Select * from Rooms where room=%s ", [selected_room])

    reservations = session.execute("Select * from reservation where seans_id =%s and occupied=True allow filtering", [selected_seans])

    reserved_places = []
    for r in reservations:
        reserved_places.append((ord(r.seat_row) - 65, r.seat_number,))

    #   print("seans: " + str(r.seans_id) + "  Room: " + str(r.room))

    number_of_seats_in_row = rooms[0].capacity / rooms[0].numberofrows
    number_of_seats_in_row = number_of_seats_in_row + 1
    for row in range(0, rooms[0].numberofrows):
        for seat in range(1, int(number_of_seats_in_row)):
            empty = True
            for ra, rb in reserved_places:
                # print(ra,rb)
                if row == ra and seat == rb:
                    print(" " + chr(65 + row) + str(seat) + "X ", end="", flush=True)
                    empty = False

            if (empty):
                print(" " + chr(65 + row) + str(seat) + " ", end="", flush=True)  # ascii translation.. not safest
        print("\n")


def Make_Reservation(selected_seans,row,seat):
    wanted_seat=session.execute("select * from reservation where seans_id=%(seans_id)s and seat_number=%(seat)s and seat_row=%(row)s allow filtering",{'seans_id' : selected_seans, 'seat' : seat,'row' : row})
    #print(wanted_seat)

    if wanted_seat==[]: #add checking if seat exists ?
        new_id = int(GetLastReservationID()) + 1
        seanses = session.execute('SELECT * FROM Seans where seans_id=%s', [selected_seans])
        print("making...reservation " + str(new_id))
        session.execute("""Insert into Reservation(reservation_id, seans_id,room ,seat_row,seat_number,occupied)
            values (%(reservation_id)s,%(seans_id)s,%(room)s,%(seat_row)s,%(seat_number)s,%(occupied)s)""",
                        {'reservation_id': new_id, 'seans_id': selected_seans, 'room': seanses[0].room, 'seat_row': row,
                         'seat_number': seat, 'occupied': True})
    elif wanted_seat[0].occupied == False: #anulowana rezerwacja
        print("making...reservation " )
        wanted_seat = session.execute(
            "select * from reservation where seat_number=%(seat)s and seat_row=%(row)s and seans_id=%(seans_id)s allow filtering",
            {'seat': seat, 'row': row, 'seans_id' : selected_seans})

        session.execute("Update reservation set occupied=True where reservation_id=%s",[wanted_seat[0].reservation_id])

    else:
        print("sorry place is occupied")




cluster = Cluster(
    ['10.10.0.1', '10.10.0.2'])

session = cluster.connect('project') #make sure it's connected

print("connected")

PrintSeanses()




PrintSeansSeatsWithReservations(1,1)

for x in range(1,5):
    Make_Reservation(3, 'A', x)
    Make_Reservation(3, 'B', x)
    Make_Reservation(3, 'C', x)
    Make_Reservation(3, 'D', x)

PrintSeansSeatsWithReservations(3,1)

#Make_Reservation(1,'A',3)

#PrintSeansSeatsWithReservations(1,1)


