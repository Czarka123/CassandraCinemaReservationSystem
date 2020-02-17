import time as t
import sys
import netifaces as ni
import random
import datetime
import threading
import uuid


from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from datetime import datetime as dt
from datetime import date


# Table Creation
#CREATE TABLE Reservation(reservation_id uuid, seans_id int, seat_number int, node varchar, room int, seat_row varchar, time timestamp, PRIMARY KEY ((seans_id),seat_number,seat_row));
#CREATE TABLE ReservationNode(reservation_id uuid, node varchar, seans_id int, seat_number int, seat_row varchar, PRIMARY KEY (reservation_id));
#CREATE TABLE AvailableSeat(id varchar primary key, seans_id int, seat_number int, seat_row varchar);
#Create Table Seans(seans_id int Primary Key, film_name text, date text, room int, all_place_occupied boolean);

# Create Table Rooms(room int Primary Key, capacity int, numberOfRows int); #ulatwienie dziala tylko kidy sale maja ta sama ilosc miejsc w kazdym rzedzie
# room 2 razy nie jest potrzebne ale wydaje mi sie e dla wygody moze zostac

#Inserts
#Insert into Rooms(room, capacity, numberOfRows) values (1, 20, 4);
#Insert into Reservation(reservation_id,seans_id,seat_number,node, room,seat_row,time) values (1,1,3,'192.168.0.15',1,'C', TIME);
#Insert into ReservationNode(reservation_id,node,seans_id,seat_number,seat_row) values (1,'192.168.0.15',1,3,'C');
#Insert into Seans(seans_id, film_name, date, room, all_place_occupied) values (1,'joker','20:00 : 3.12.2019',1,false);

def PrintSeanses():
    seanses = session.execute('SELECT *  FROM Seans')

    #places = session.execute("select * from Rooms")
    for s in seanses:
        reservations = session.execute(
            "select count(*) from reservation where seans_id=%s", [s.seans_id]) #not most optimal if there are a lot of senses, but good for testing
        print ("ID: "+str(s.seans_id)+" Film: "+s.film_name +"  Date: " +s.date+"  Room: " +str(s.room) +" reservations: " +str(reservations[0].count))

def GetLastReservationID():
    latestentry=session.execute('SELECT Max(reservation_id)  FROM Reservation ')
   # print(" last "+str(latestentry[0])) #not most elegant...mona sprbowac loswac id zamaiast dawac kolejne
    return latestentry[0].system_max_reservation_id

def GetLastSeansID():
    latestentry=session.execute('SELECT Max(seans_id)  FROM Seans ')
    #print(" last "+str(latestentry[0].system_max_seans_id)) #not most elegant...mona sprbowac loswac id zamaiast dawac kolejne
    return latestentry[0].system_max_seans_id

def GetRandomReservationID():
    id = uuid.uuid1()
    return id

def RegisterSeans(filmname, newdate, roomnumber):
    seansid = GetLastSeansID()
    if (seansid != None): seansid = seansid + 1
    else: seansid = 1

    session.execute("""Insert into Seans(seans_id, film_name, date, room, all_place_occupied) 
    values (%(seans_id)s,%(film_name)s,%(date)s,%(room)s,%(all_place_occupied)s)""",
                    {'seans_id': seansid, 'film_name': filmname, 'date': newdate, 'room': roomnumber,
                     'all_place_occupied': False})

    room = session.execute("Select * from rooms where room=%s ", [roomnumber])
    seats_per_row =room[0].capacity/ room[0].numberofrows
    for row in range(0,room[0].numberofrows):
        for seat in range(1, int(seats_per_row)+1):
            session.execute("""Insert into AvailableSeat(id, seans_id, seat_number, seat_row) values (%(id)s,%(seans_id)s,%(seat_number)s,%(seat_row)s)""",                {'id': str(seansid)+chr(65+row)+str(seat), 'seans_id': seansid, 'seat_number': seat, 'seat_row': chr(65+row)})
    print("you've added seans")

def PrintSeansSeatsWithReservations(selected_seans):


    reservations = session.execute("Select * from reservation where seans_id =%s", [selected_seans])

    if(reservations==[]): #printing empty room
        return

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

def Make_Reservation(selected_seans,row,seat, nodeid):
    room = session.execute("Select * from Seans where seans_id=%(seans_id)s", {'seans_id' : selected_seans})
    if not room:return #seans
    rooms = session.execute("Select * from Rooms where room=%s", [room[0].room])
    number_of_seats_in_row = rooms[0].capacity / rooms[0].numberofrows

    if seat>number_of_seats_in_row: return

    rowexist=0

    for rowcount in range(0, rooms[0].numberofrows):
        if row==chr(65 + rowcount): rowexist=1
    if rowexist==0: return

    wanted_seat=session.execute("select * from reservation where seans_id=%(seans_id)s and seat_number=%(seat)s and seat_row=%(row)s",{'seans_id' : selected_seans, 'seat' : seat,'row' : row})


    if wanted_seat==[]: #add checking if seat exists ?
        new_id = GetRandomReservationID()
        #if (new_id != None): new_id = new_id + 1
        #else: new_id = 1
        seanses = session.execute('SELECT * FROM Seans where seans_id=%s', [selected_seans])

       #making sure that got latest id
        # while True:
        #     new_id = GetLastReservationID()
        #     new_id = new_id + 1
        #     it = 0
        #     check_reservationid = session.execute(
        #         "select * from reservationnode where reservation_id=%(reservation_id)s",
        #         {'reservation_id': new_id})
        #
        #     for check in check_reservationid:
        #         it = it + 1
        #     if it == 0:
        #         break

        print(nodeid + " making...reservation " + str(new_id))

        #no timestamps here I think last write wins here
        session.execute("""Insert into ReservationNode(reservation_id,node,seans_id,seat_number,seat_row) values (%(reservation_id)s,%(node)s,%(seans_id)s,%(seat_number)s,%(seat_row)s)""",  {'reservation_id': new_id,'node' : nodeid,'seans_id': selected_seans,'seat_number': seat, 'seat_row': row})
        t.sleep(0.1)

        #check_reservation = session.execute(
        #    "select * from reservation where reservation_id=%(reservation_id)s",
        #   {'reservation_id': new_id})
        check_reservation = session.execute(
             "select * from reservationnode where reservation_id=%(reservation_id)s",
           {'reservation_id': new_id})

        if check_reservation[0].node == nodeid: #check if reservation is not doubled

            session.execute("""Insert into Reservation(reservation_id,seans_id,seat_number,node,room ,seat_row, time)
                                    values (%(reservation_id)s,%(seans_id)s,%(seat_number)s,%(node)s,%(room)s,%(seat_row)s,%(time)s)""",
                            {'reservation_id': new_id, 'seans_id': selected_seans, 'seat_number': seat,
                             'node': nodeid, 'room': seanses[0].room, 'seat_row': row, 'time': dt.now()})
            session.execute("delete from AvailableSeat where id=%(id)s",
                            {'id': str(selected_seans) + str(row) + str(seat)})

            t.sleep(0.1)
            check_reservation2 = session.execute(
                "select * from reservation where seat_row=%(seat_row)s and seat_number=%(seat_number)s and seans_id = %(seans_id)s",
                {'seat_row': row, 'seat_number': seat, 'seans_id': selected_seans})

            it=0
            for check in check_reservation2:
                it=it+1

            if it>1:
                #nawet jeeli nigdy tu nie wchodzi to teoretycznie moze i trzeba prownujac timestamp anulowac
                Cancel_Reservation(new_id,nodeid)
                print("note: reservation cancelled due to timestamp")
                return False
            else:

                print("reservation complete")
                return True

        #else :
         #   print("Failed to make reservation try again")
            
   #else :
    return False

def Cancel_Reservation(reservation_id, nodeid):
    print("cancelling starting.........................................")
    reservation_id=uuid.UUID(reservation_id)
    wanted_seat=session.execute("select * from reservationnode where reservation_id=%(reservation_id)s",{'reservation_id' : reservation_id})
    if not wanted_seat: return
    check_reservation2 = session.execute(
                "select * from reservation where seat_row=%(seat_row)s and seat_number=%(seat_number)s and seans_id = %(seans_id)s",
                {'seat_row': wanted_seat[0].seat_row, 'seat_number': wanted_seat[0].seat_number, 'seans_id': wanted_seat[0].seans_id})

    it=0
    for check in check_reservation2:
        it=it+1

    if it>1:
       max_time=session.execute("select max(time) from reservation where seans_id=%(seans_id)s and seat_number=%(seat_number)s and seat_row=%(seat_row)s",{'seans_id' : check_reservation2[0].seans_id, 'seat_number' : check_reservation2[0].seat_number, 'seat_row' : check_reservation2[0].seat_row})
       session.execute("delete from reservation where seans_id=%(seans_id)s and seat_number=%(seat_number)s and seat_row=%(seat_row)s and time=%(time)s",{'seans_id' : check_reservation2[0].seans_id, 'seat_number' : check_reservation2[0].seat_number, 'seat_row' : check_reservation2[0].seat_row, 'time' : max_time[0].time})
       session.execute("delete from reservationnode where reservation_id=%(reservation_id)s",{'reservation_id' : check_reservation2[0].reservation_id})
    else:    
         if wanted_seat[0].node!=str(nodeid):
            print("can't remove reservation, this node didn't make reservation")
            return
         session.execute("""Insert into AvailableSeat(id, seans_id, seat_number, seat_row) values (%(id)s, %(seans_id)s,%(seat_number)s,%(seat_row)s)""",                {'id': str(wanted_seat[0].seans_id)+ str(wanted_seat[0].seat_row)+str(wanted_seat[0].seat_number),'seans_id': wanted_seat[0].seans_id, 'seat_number': wanted_seat[0].seat_number, 'seat_row': wanted_seat[0].seat_row})
         session.execute("delete from reservation where seans_id=%(seans_id)s and seat_number=%(seat_number)s and seat_row=%(seat_row)s",{'seans_id' : wanted_seat[0].seans_id, 'seat_number' : wanted_seat[0].seat_number, 'seat_row' : wanted_seat[0].seat_row})
         session.execute("delete from reservationnode where reservation_id=%(reservation_id)s",{'reservation_id' : reservation_id})
    print("reservation %s canceled!!!",[reservation_id])

def Force_Cancel_Reservation(reservation_id):
    print("cancelling starting!!!")
    reservation_id = uuid.UUID(reservation_id)
    wanted_seat = session.execute("select * from reservationnode where reservation_id=%(reservation_id)s",
                                  {'reservation_id': reservation_id})
    if not wanted_seat: return

    session.execute(
        """Insert into AvailableSeat(id, seans_id, seat_number, seat_row) values (%(id)s, %(seans_id)s,%(seat_number)s,%(seat_row)s)""",
        {'id': str(wanted_seat[0].seans_id) + str(wanted_seat[0].seat_row) + str(wanted_seat[0].seat_number),
         'seans_id': wanted_seat[0].seans_id, 'seat_number': wanted_seat[0].seat_number,
         'seat_row': wanted_seat[0].seat_row})
    session.execute(
        "delete from reservation where seans_id=%(seans_id)s and seat_number=%(seat_number)s and seat_row=%(seat_row)s",
        {'seans_id': wanted_seat[0].seans_id, 'seat_number': wanted_seat[0].seat_number,
         'seat_row': wanted_seat[0].seat_row})
    session.execute("delete from reservationnode where reservation_id=%(reservation_id)s",
                    {'reservation_id': reservation_id})

class TestThread (threading.Thread) :
    def __init__(self, seans , ip):
        threading.Thread.__init__(self)
        self.seans=seans
        self.ip=ip

    def run(self):
        print("starting thread with ipnode :" + self.ip)
        stressTestFunction1(self.seans, self.ip)

# class TestThread2 (threading.Thread) :
#     def __init__(self, seans , ip, numberOf):
#         threading.Thread.__init__(self)
#         self.seans=seans
#         self.ip=ip
#         self.numberOf=numberOf
#
#
#     def run(self):
#         print("starting thread with ipnode :" + self.ip)
#         stressTestFunction3(self.seans, self.ip,self.numberOf)

#function for removing random
# def stressTestFunction3(seansid,ip,numberOf) :
#     time_start = dt.now().time()
#     se = session.execute("Select * from seans where seans_id =%s ", [seansid])
#     if se != []:
#         reservations = session.execute("select reservation_id from reservation where seans_id=%s", [seansid])
#         array_available=[]
#         for row in reservations:
#          array_available.append(row[0])
#          #print("to free " + str(row[0]))
#
#         for x in array_available :
#             print("to free "+ str(x))



def stressTestFunction2(seansid,ip): #seats in order
    time_start = dt.now().time()
    se = session.execute("Select * from seans where seans_id =%s ", [seansid])
    if se != []:
        room = session.execute("Select * from rooms where room=%s ", [se[0].room])
        seats_per_row = room[0].capacity / room[0].numberofrows
        seats_per_row = seats_per_row + 1

        notcompleted = 0
        #while (reservationscount[0].count < room[0].capacity):
        for row in range(0,room[0].numberofrows):
            for seat in range(1,int(seats_per_row)):
                status = Make_Reservation(seansid, chr(65+row), seat,
                                          ip)
                if status == False:
                    notcompleted = notcompleted + 1
                    print("seat was already reserved.......")



        time_end = dt.now().time()
        time = dt.combine(datetime.date.today(), time_end) - dt.combine(datetime.date.today(), time_start)
        print("Thread: " + ip + " time: ", time)
        print("Thread: " + ip + " not completed reservations: ", notcompleted)



def stressTestFunction1(seansid,ip): #seats random

    time_start = dt.now().time()
    se = session.execute("Select * from seans where seans_id =%s ", [seansid])
    if se != []:
        room = session.execute("Select * from rooms where room=%s ", [se[0].room])
        seats_per_row = room[0].capacity / room[0].numberofrows
        seats_per_row = seats_per_row + 1
        reservationscount = session.execute("select count(*) from reservation where seans_id=%s", [seansid])
        fill_array = session.execute("select * from AvailableSeat")
        array_available = []
        for row in fill_array:
            if row.id.startswith(str(seansid)):
                array_available.append(row.id)

        notcompleted = 0
        while (reservationscount[0].count < room[0].capacity):
            rand_seat = random.choice(array_available)
            rand_seat_row = session.execute("select * from AvailableSeat where id=%s", [rand_seat])
            if rand_seat_row != []:
                status = Make_Reservation(seansid, rand_seat_row[0].seat_row, rand_seat_row[0].seat_number,
                                          ip)
                if status == True:
                    array_available.remove(rand_seat)
                else:
                    notcompleted = notcompleted + 1
                    print("seat was already reserved...please select another one")
            reservationscount = session.execute("select count(*) from reservation where seans_id=%s", [seansid])
        time_end = dt.now().time()
        time = dt.combine(datetime.date.today(), time_end) - dt.combine(datetime.date.today(), time_start)
        print("Thread: "+ip + " time: ", time)
        print("Thread: "+ip + " not completed reservations: ", notcompleted)




def DisplayOptions () :
    print("0 - help")
    print("-1 - exit")
    print("1 - see all seanses")
    print("2 - see all seats for selected seans")
    print("3 - add new seans")
    print("4 - make reservation")
    print("5 - cancel reservation")
    print("6 - stress test")

cluster = Cluster(['10.10.0.1', '10.10.0.2'])
interface='eth1'

print("connecting to database")
session = cluster.connect('project') #make sure it's connected
        
print("connected")

print("Welcome to Cinema Reservation System")


print("What do you want do do (press 0 to see options) :")

choice=0
ni.ifaddresses(interface)
nodeid=ni.ifaddresses(interface)[ni.AF_INET][0]['addr']

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
        reservation_choice = int(input("type number of reservations you want make or type 0 to reserve all seats  "))
        if (reservation_choice == 0):
            seans = int(input("type  seans id:  "))
            time_start=dt.now().time()
            se = session.execute("Select * from seans where seans_id =%s ", [seans])
            if se!=[]:
                room = session.execute("Select * from rooms where room=%s ", [se[0].room])
                seats_per_row =room[0].capacity/ room[0].numberofrows
                seats_per_row=seats_per_row+1
                reservationscount = session.execute("select count(*) from reservation where seans_id=%s", [seans])

                fill_array=session.execute("select * from AvailableSeat")
                array_available=[]
                for row in fill_array:
                    if row.id.startswith(str(seans)):
                       array_available.append(row.id)

                notcompleted=0

                while(reservationscount[0].count<room[0].capacity):
                    rand_seat=random.choice(array_available)
                    rand_seat_row=session.execute("select * from AvailableSeat where id=%s", [rand_seat])
                    if rand_seat_row != []:
                       status = Make_Reservation(seans, rand_seat_row[0].seat_row, rand_seat_row[0].seat_number, nodeid)
                       if status==True:
                          array_available.remove(rand_seat)
                       else:
                          notcompleted=notcompleted+1
                          print("seat was already reserved...please select another one")
                    reservationscount = session.execute("select count(*) from reservation where seans_id=%s", [seans])

            time_end=dt.now().time()
            time = dt.combine(datetime.date.today(),time_end) - dt.combine(datetime.date.today(),time_start)
            print("time: ",time)
            print("not completed reservations: ", notcompleted)
        else :
                 seans = int(input("type  seans id:  "))
                 for i in range(reservation_choice):
                    row = str(input("type selected row:  "))
                    seat = int(input("type selected seat:  "))
                    status = Make_Reservation(seans,row,seat, nodeid)
                    if (status==False):
                       fill_array=session.execute("select * from AvailableSeat")
                       array_available=[]
                       startswith=str(seans)+str(row)
                       for row in fill_array:
                           if row.id.startswith(startswith):
                              array_available.append(row.id)
                       if (len(array_available)==0):
                          for row in fill_array:
                              if row.id.startswith(str(seans)):
                                 array_available.append(row.id)
                       if (len(array_available)==0):
                          print("seat was already reserved...please select another one in other seans")
                       else:
                          rand_seat=random.choice(array_available)
                          rand_seat_row=session.execute("select * from AvailableSeat where id=%s", [rand_seat])
                          print("seat was already reserved...please select another one..."+str(rand_seat_row[0].seat_row)+str(rand_seat_row[0].seat_number)+" is available")

    elif(choice==5):
        can_id = input("type reservation id that you want to cancel:  ")
        Cancel_Reservation(can_id, nodeid)
    elif(choice==6):


        RegisterSeans('Test', '00:00:0000 :0:0', 1)
        test_seans_id1 = GetLastSeansID()
        thread1 = TestThread(test_seans_id1, '10.10.0.3')
        thread2 = TestThread(test_seans_id1, '10.10.0.4')

        thread1.start()
        thread2.start()

        RegisterSeans('Test2', '00:00:0000 :0:0', 1)
        test_seans_id2 = GetLastSeansID()
        thread3 = TestThread(test_seans_id2, '10.10.0.5')
        thread4 = TestThread(test_seans_id2, '10.10.0.6')
        thread5 = TestThread(test_seans_id2, '10.10.0.7')

        thread3.start()
        thread4.start()
        thread5.start()

        RegisterSeans('Test3', '00:00:0000 :0:0', 1)
        test_seans_id3 = GetLastSeansID()
        thread6 = TestThread(test_seans_id3, '10.10.0.9')
        thread7 = TestThread(test_seans_id3, '10.10.0.10')
        thread8 = TestThread(test_seans_id3, '10.10.0.11')
        thread9 = TestThread(test_seans_id3, '10.10.0.12')
        thread10 = TestThread(test_seans_id3, '10.10.0.13')

        thread6.start()
        thread7.start()
        thread8.start()
        thread9.start()
        thread10.start()

        #t.sleep(7)

        RegisterSeans('Test4', '00:00:0000 :0:0', 1)
        test_seans_id4 = GetLastSeansID()


        thread11 = TestThread(test_seans_id4, '10.10.0.13')
        thread12 = TestThread(test_seans_id4, '10.10.0.14')
        thread13 = TestThread(test_seans_id4, '10.10.0.15')

        thread11.start()
        thread12.start()
        thread13.start()       


    elif (choice == 7):
        seans_id = int(input("type seans id that you want to display:  "))
        seans_rows = session.execute("select seans_id,seat_row,seat_number,reservation_id from reservation where seans_id=%s;", [seans_id])

        for sr in seans_rows:
            print(str(sr[0]) +" | "+sr[1]+" : "+str(sr[2]) + " -> "+str(sr[3]))
    
    elif (choice ==8):
        threadarray=[]
        RegisterSeans('Test5', '00:00:0000 :0:0', 1)
        test_seans_id5 = GetLastSeansID()
        for i in range(1,1000):
            ip="10.10.0.%d" % (i+13)
       	    thread=TestThread(test_seans_id5,ip)
            threadarray.append(thread)
       	for thr in threadarray:
            thr.start()
    
    else :
        continue
