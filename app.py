#################################################
# Climate App 
#################################################
#Dependencies
import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.pool import StaticPool
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
# Web sites use threads, but sqlite is not thread-safe.
# These parameters will let us get around it.
# However, it is recommended you create a new Engine, Base, and Session
#   for each thread (each route call gets its own thread)
engine = create_engine("sqlite:///Resources/hawaii.sqlite",
    connect_args={'check_same_thread':False},
    poolclass=StaticPool)
#################################################
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)


# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)
#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# All the functions
#################################################
# Calculate the date 1 year ago from the last data point in the database
def prior_date():
    # Latest Date
    last_date = session.query(Measurement.date).\
    order_by(Measurement.date.desc()).first()
    # Read only the date in string
    last_date_value = last_date[0]
    # convert the string in date and '%Y-%m-%d' format
    read_date = pd.to_datetime(last_date_value, format="%Y-%m-%d").date()
    # Get one year back date
    query_date = read_date.replace(year=(read_date.year - 1))
    return (query_date)
#---------------------------------------------------
# Output all the results as a list
def output_results(results):
    result_list =[]
    result_dict = {}

    for r in results:
        result_dict = {r[0]:r[1]}
        result_list.append(result_dict)
    # return to user
    return (result_list)
#---------------------------------------------------
# Calculate temp in given date range
def temp_date_range(temp_start_date,temp_end_date):
    temp_dates = calc_temps(temp_start_date,temp_end_date)
    temp_dict ={}
    for tem in temp_dates:
        temp_dict = {"TMIN": tem[0],"TAVG" : tem[1], "TMAX" : tem[2]}
    return (temp_dict)
#---------------------------------------------------
# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d    
    Returns:
        TMIN, TAVE, and TMAX
    """
    calc_all_temps = session.query(
        func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        filter(Measurement.date <= end_date).all()  
    return calc_all_temps

#################################################
# Flask Routes
#################################################
# Homepage
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Honolulu, Hawaii Weather Data API<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"  
        f"/api/v1.0/2017-01-01<br/>"
        f"/api/v1.0/2017-01-01/2017-01-08"
    )
#---------------------------------------------------

#route: precipitation
@app.route("/api/v1.0/precipitation")
#Design a query to retrieve the last 12 months of precipitation data.
#Convert the query results to a Dictionary using `date` as the key and `prcp` as the value.
 # Return the JSON representation of your dictionary.
def precipitation():
    
    # Read 1 year prior date
    prior_year_date = prior_date()

    #Query all data in 1 year from measurement table
    # Perform a query to retrieve the data and precipitation scores for 1 year

    results_prec = session.query(Measurement.date,Measurement.prcp).\
    filter(Measurement.date >= prior_year_date).order_by(Measurement.date).all()
    #Create a dictionary from the row data and append to a list of all Measurement  
    prec_list= output_results(results_prec)   
    return jsonify(prec_list)
#---------------------------------------------------

#route: stations
@app.route("/api/v1.0/stations")
#Read all stations and Return a JSON list of stations from the dataset
def stations():
     #list for station data
    all_stations = []
    # query to read all stations
    stations = session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()
    #create a dictionary for each station's info and append to list
    for station in stations:
        station_dict = {"Station id": station[0], "Station name": station[1], "latitude": station[2], "longitude": station[3], "elevation": station[4]}
        all_stations .append(station_dict)
    # return to user
    return jsonify(all_stations)
#---------------------------------------------------

#route: tobs
@app.route("/api/v1.0/tobs")
#* query for the dates and temperature observations from a year from the last data point.
# * Return a JSON list of Temperature Observations (tobs) for the previous year.
def tobs():
     # Read 1 year prior date
    prior_tobs_date = prior_date()
    tobs_result = session.query(Measurement.date,Measurement.tobs).\
    filter(Measurement.date >= prior_tobs_date).order_by(Measurement.date).all() 
    tobs_list = output_results(tobs_result)  
    return jsonify(tobs_list)
#---------------------------------------------------

 #route: <start> : start date = 2017-01-01 
@app.route("/api/v1.0/<start>")
#Return a JSON list of the minimum temperature, the average temperature, 
# and the max temperature for a given start date
def temp(start):
    # input start date
    start_date = start.replace(" ","")
    # Read last date
    last_date_temp = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    last_date_val = str(last_date_temp)[2:-3]  
    # calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.
    temp_result = temp_date_range(start_date,last_date_val)
    return(jsonify(temp_result))    
#---------------------------------------------------

#route: <start>/<end> : start date = 2017-01-01 and end date = 2017-01-08
@app.route("/api/v1.0/<start>/<end>")
#Return a JSON list of the minimum temperature, the average temperature, 
# and the max temperature for a given start-end range.
# calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive. 
def temp_range(start,end):
    start_date = start.replace(" ","")
    end_date = end.replace(" ","")
    temp_start_end_date_results = temp_date_range(start_date,end_date)
    return(jsonify(temp_start_end_date_results))
#################################################

if __name__ == "__main__":
    app.run(debug=True)