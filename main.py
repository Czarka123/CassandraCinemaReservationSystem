import time

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

import netifaces as ni

# Table Creation
#CREATE TABLE Reservation(reservation_id int PRIMARY KEY, node varchar, seans_id int, room int, seat_row varchar, seat_number int);
#Create Table Seans(seans_id int Primary Key, film_name text, date text, room int, all_place_occupied boolean);

# Create Table Rooms(room int Primary Key, capacity int, numberOfRows int); #ulatwienie dziala tylko kidy sale maja ta sama ilosc miejsc w kazdym rzedzie
# room 2 razy nie jest potrzebne ale wydaje mi sie e dla wygody moze zostac

#Inserts
#Insert into Rooms(room, capacity, numberOfRows) values (1, 20, 4);
#Insert into Reservation(reservation_id,node,seans_id, room,seat_row,seat_number) values (1,'192.168.0.15',1,1,'C',3);
#Insert into Seans(seans_id, film_name, date, room, all_place_occupied) values (1,'joker','20:00 : 3.12.2019',1,false);


def PrintSeanses():
    seanses = session.execute('SELECT *  FROM Seans')

    #places = session.execute("select * from Rooms")
    for s in seanses:
        reservations = session.execute(
            "select count(*) from reservation where seans_id=%s allow filtering", [s.seans_id]) #not most optimal if there are a lot of senses, but good for testing
        print ("ID: "+str(s.seans_id)+" Film: "+s.film_name +"  Date: " +s.date+"  Room: " +str(s.room) +" reservations: " +str(reservations[0].count))

def GetLastReservationID():
    latestentry=session.execute('SELECT Max(reservation_id)  FROM Reservation ')
    if not latestentry : return 0
   # print(" last "+str(latestentry[0])) #not most elegant...mona sprbowac loswac id zamaiast dawac kolejne
    return latestentry[0].system_max_reservation_id

def GetLastSeansID():
    latestentry=session.execute('SELECT Max(seans_id)  FROM Seans ')

    #print(" last "+str(latestentry[0].system_max_seans_id)) #not most elegant...mona sprbowac loswac id zamaiast dawac kolejne
    return latestentry[0].system_max_seans_id


def RegisterSeans(filmname, newdate, roomnumber):
    seansid = GetLastSeansID()
    seansid = seansid + 1

    session.execute("""Insert into Seans(seans_id, film_name, date, room, all_place_occupied) 
    values (%(seans_id)s,%(film_name)s,%(date)s,%(room)s,%(all_place_occupied)s)""",
                    {'seans_id': seansid, 'film_name': filmname, 'date': newdate, 'room': roomnumber,
                     'all_place_occupied': False})
    print("you've added seans")

def PrintSeansSeatsWithReservations(selected_seans):


    reservations = session.execute("Select * from reservation where seans_id =%s allow filtering", [selected_seans])

    if(reservations==[]): #printing empty room
        return
        #reservations=session.execute("Select * from seans where seans_id =%s", [selected_seans])
        #rooms = session.execute("Select * from Rooms where room=%s ", [reservations[0].room])
        #print(" Room: " + str([reservations[0].room]))
        #number_of_seats_in_row = rooms[0].capacity / rooms[0].numberofrows
        #number_of_seats_in_row = number_of_seats_in_row + 1
        #for row in range(0, rooms[0].numberofrows):
        #    for seat in range(1, int(number_of_seats_in_row)):
        #        print(" " + chr(65 + row) + str(seat) + " ", end="", flush=True)

        #    print("\n")
        #return;


    rooms = session.execute("Select * from Rooms where room=%s ", [reservations[0].room])

    print(" Room: "+str( [reservations[0].room]))
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
    room = session.execute("Select * from Seans where seans_id=%(seans_id)s", {'seans_id' : selected_seans})
    if not room:return #seans
    rooms = session.execute("Select * from Rooms where room=%s", [room[0].room])
    number_of_seats_in_row = rooms[0].capacity / rooms[0].numberofrows

    if seat>number_of_seats_in_row: return

    rowexist=0

    for rowcount in range(0, rooms[0].numberofrows):
        if row==chr(65 + rowcount): rowexist=1
    if rowexist==0: return

    wanted_seat=session.execute("select * from reservation where seans_id=%(seans_id)s and seat_number=%(seat)s and seat_row=%(row)s allow filtering",{'seans_id' : selected_seans, 'seat' : seat,'row' : row})


    if wanted_seat==[]: #add checking if seat exists ?
        new_id = (GetLastReservationID()) + 1
        ni.ifaddresses('enp0s9')
        nodeid=ni.ifaddresses('enp0s9')[ni.AF_INET][0]['addr']
        seanses = session.execute('SELECT * FROM Seans where seans_id=%s', [selected_seans])
        print("making...reservation " + str(new_id))
        session.execute("""Insert into Reservation(reservation_id,node, seans_id,room ,seat_row,seat_number)
            values (%(reservation_id)s,%(node)s,%(seans_id)s,%(room)s,%(seat_row)s,%(seat_number)s)""",
                        {'reservation_id': new_id, 'node' : nodeid,'seans_id': selected_seans, 'room': seanses[0].room, 'seat_row': row,
                         'seat_number': seat})
        time.sleep(0.1)

        check_reservation = session.execute(
            "select * from reservation where reservation_id=%(reservation_id)s allow filtering",
            {'reservation_id': new_id})

        if check_reservation[0].node == nodeid: #check if reservation is not doubled
            check_reservation2 = session.execute(
                "select * from reservation where seat_row=%(seat_row)s and seat_number=%(seat_number)s and seans_id = %(seans_id)s allow filtering",
                {'seat_row': row, 'seat_number': seat, 'seans_id': selected_seans})

            it=0
            for check in check_reservation2:
                it=it+1

            if it>1:
                Cancel_Reservation(new_id)
            else:
                print("reservation complete")


        else :
            print("seat was already reserved...please select another one")
    else :
        print("seat was already reserved...please select another one")

    #elif wanted_seat[0].occupied == False: #anulowana rezerwacja
     #   print("making...reservation " )
      #  wanted_seat = session.execute(
       #     "select * from reservation where seat_number=%(seat)s and seat_row=%(row)s and seans_id=%(seans_id)s allow filtering",
        #    {'seat': seat, 'row': row, 'seans_id' : selected_seans})

       # session.execute("Update reservation set occupied=True where reservation_id=%s",[wanted_seat[0].reservation_id])

   # else:
    #    print("sorry place is occupied")
def Cancel_Reservation(reservation_id):
    ni.ifaddresses('enp0s9')
    nodeid=ni.ifaddresses('enp0s9')[ni.AF_INET][0]['addr']
    wanted_seat=session.execute("select * from reservation where reservation_id=%(reservation_id)s allow filtering",{'reservation_id' : reservation_id})
    if not wanted_seat: return
    if wanted_seat[0].node!=str(nodeid):
        print("can't remove reservation, this node didn't make reservation")
        return

    session.execute("delete from reservation where reservation_id=%(reservation_id)s",{'reservation_id' : reservation_id})
    print("reservation %s canceled",[reservation_id])

def DisplayOptions () :
    print("0 - help")
    print("-1 - exit")
    print("1 - see all seanses")
    print("2 - see all seats for selected seans")
    print("3 - add new seans")
    print("4 - make reservation")
    print("5 - cancel reservation")


cluster = Cluster(
    ['10.10.0.1', '10.10.0.2'])


print("connecting to database")
session = cluster.connect('project') #make sure it's connected

print("connected")

print("Welcome to Cinema Reservation System")
print("What do you want do do (press 0 to see options) :")

choice=0

while (choice!=-1) :
    choice=int(input("awaiting key:  "))

    if(choice==0):
        DisplayOptions();
    elif(choice==1):
        PrintSeanses()
    elif(choice==2):
        seans=int(input("type seans_id:  "))
        PrintSeansSeatsWithReservations(seans)
    elif (choice == 3):
        film=str(input("type film name:  "))
        date=str(input("type date:  "))
        room=int(input("type room number:  "))
        RegisterSeans(film,date,room)
    elif (choice == 4):
        reservation_choice = int(input("type 1 if you are making single reservation, 2 all seats  "))
        if (reservation_choice == 1):
            seans = int(input("type  seans id:  "))
            row = str(input("type selected row:  "))
            seat = int(input("type selected seat:  "))
            Make_Reservation(seans,row,seat)
        elif (reservation_choice == 2):
            seans = int(input("type  seans id:  "))
            se = session.execute(
                "Select * from seans where seans_id =%s ", [seans])
            if se!=[]:
                room = session.execute("Select * from Rooms where room=%s ", [se[0].room])

                seats_per_row =room[0].capacity/ room[0].numberofrows
                seats_per_row=seats_per_row+1
                for row in range(0,room[0].numberofrows):
                    for seat in range(1, int(seats_per_row)):
                        Make_Reservation(seans, chr(65 + row), seat)
    elif (choice==5):
        can_id =int(input("type reservation id that you want to cancel:  "))
        Cancel_Reservation(can_id)
    else :
        continue