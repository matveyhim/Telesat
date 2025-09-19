import os
from os import listdir
from os.path import  join
from datetime import datetime
from PIL import Image, ImageMath
import io
import zipfile
import traceback

import json
import time
import difflib

import telebot
from telebot import apihelper

# apihelper.API_URL = "http://127.0.0.1:8081/bot{0}/{1}" # for local api server
bot = telebot.TeleBot('token')

logID = 00000
dataID = -00000


output_folder = 'live_output'
location = ''
maxTime = 60 # max store time (minutes) 

downlinks=[dict(downlink = 'NOAA APT', dataname = '/', imgdir='', preview = [dict(name = 'raw_sync.png', use = 'all')]),
           dict(downlink = 'NOAA DSB', dataname = '/', imgdir = 'HIRS', preview = [dict(name = 'hirs_rgb_HIRS_False_Color.png', use = 'all')]),

           dict(downlink = 'Meteor LRPT', dataname = '/', imgdir = 'MSU-MR', preview = [dict(name = 'msu_mr_rgb_AVHRR_3a21_False_Color_corrected.png', use = 'all'), dict(name = 'msu_mr_rgb_MSU-MR_124_False_Color_corrected.png', use = 'all')]),

           dict(downlink = 'NOAA HRPT', dataname = 'noaa_hrpt.raw16', imgdir = 'AVHRR', preview = [dict(name = 'avhrr_3_rgb_AVHRR_124_False_Color_corrected.png', use = 'day'), dict(name = 'avhrr_3_rgb_AVHRR_3b45_IR_False_Color_corrected.png', use = 'night')]),
           dict(downlink = 'Meteor HRPT', dataname = 'meteor_hrpt.cadu', imgdir = 'MSU-MR', preview = [dict(name = 'msu_mr_rgb_AVHRR_3a21_False_Color_corrected.png', use = 'day'), dict(name = 'msu_mr_rgb_AVHRR_3b45_IR_False_Color_corrected.png', use = 'night')]),
           dict(downlink = 'MetOp AHRPT', dataname = 'metop_ahrpt.cadu', imgdir = 'AVHRR', preview = [dict(name = 'avhrr_3_rgb_AVHRR_3a21_False_Color_corrected.png', use = 'day'), dict(name = 'avhrr_3_rgb_AVHRR_3b45_IR_False_Color_corrected.png', use = 'night')])]
           
        #    dict(downlink = 'AWS DB', dataname = 'aws_pfm.cadu', imgdir = 'STERNA', preview = [dict(name = 'sterna_rgb_AMSU_False_Color.png', use = 'all')]),
        #    dict(downlink = 'AWS DUMP', dataname = 'aws_pfm.cadu', imgdir = 'STERNA_Dump', preview = [dict(name = 'sterna_rgb_AMSU_False_Color.png', use = 'all')])]
a=0
a=0
errorCounter=0
defectCounter=0
processed=[]
unproc=[]
folders = [ f.path for f in os.scandir(output_folder) if f.is_dir() ]
processed = folders
print("Program started")
print("existing folders:", len(folders))
bot.send_message(logID, ("Started, "+str(len(folders))+" folders exist there V3"))

def sortByDate(a, output_folder):
    def strToDate(dstring):
        date_string=dstring[len(output_folder)+1:len(output_folder)+17]
        # print("dstr",dstring)
        return datetime.strptime(date_string, '%Y-%m-%d_%H-%M')
    return sorted(a, key=strToDate)

def tolist(iset, output_folder):
    setlist=[]
    if iset!=set():
        for item in iset:
            setlist.append(item)
        setlist=sortByDate(setlist, output_folder)
    return setlist

def tojpg(imagepath):
    foo = Image.open(imagepath)

    x,y=foo.size
    if x>2000 and y>2000:
        foo = foo.resize((int(x/2),int(y/2)),Image.LANCZOS)

    if x<400 and y<400:
            foo = foo.resize((int(x*4),int(y*4)),Image.NEAREST)

    if foo.mode in ('I'):
        foo = ImageMath.eval('foo/256', {'foo':foo}).convert('L')
    foo = foo.convert('RGB')
 
    b = io.BytesIO()
    foo.save(b, 'JPEG')
    b.seek(0)
 
    return b

def findFolders():
    
    global processed
    global unproc
    global errorCounter
    global defectCounter
    folders = [ f.path for f in os.scandir(output_folder) if f.is_dir() ]

    differences = set(difflib.ndiff(processed, folders))
    moved = set([item[2:] for item in differences if item[0]=='+' and '-' + item[1:] in differences])
    added = set([item[2:] for item in differences if item[0]=='+']) - moved

    unproc+=tolist(added, output_folder)
    processed=folders
    
    try:
        if unproc!=[]:
            print("added folders:",len(unproc),"\/")
            a=0
            for q in range(0,len(unproc)):
                product = unproc[a]
                filelist = [f for f in listdir(product) if join(product, f)]
                print("   watch folder:", product, "index:", a)

                if ('dataset.json' in filelist):
                    data = json.load(open(product+'/dataset.json','rb'))
                    print(product,output_folder)
                    passtime=(datetime.strptime(product[len(output_folder)+1:len(output_folder)+17], '%Y-%m-%d_%H-%M').strftime('%Y-%m-%d %H:%M'))
                    sat=data["satellite"]
                    folder=product[len(output_folder)+1:]
                    print("     dataset.json there")


                    for dl in downlinks:
                        for preview in dl['preview']:
                            imgname = preview['name']

                            if os.path.exists(join(product, dl['dataname'])) and os.path.exists(join(product, dl['imgdir'], imgname)):  
                                defined = True                  
                                break
                            else:
                                defined = False

                        if defined:
                            break
                        

                    if defined:
                        img = Image.open(join(product, dl['imgdir'], imgname))
                        lines = str(img.size[1])
                        image=tojpg(join(product, dl['imgdir'], imgname))

                        fileio = io.BytesIO()
                        zf=zipfile.ZipFile(fileio,'w',compression=zipfile.ZIP_DEFLATED, compresslevel=9)

                        if dl['dataname'] == '/': 
                            for root, dirs, files in os.walk(product):
                                for file in files:
                                    zf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), os.path.join(product)))
                        else:
                            s=open(join(product, dl['dataname']),'rb')
                            zf.writestr(dl['dataname'],data=s.read())
                        zf.close()
                        fileio.name = folder+'.zip'
                        fileio.seek(0)

                        
                        bot.send_photo(dataID, image ,('Location: ' +location+ ' \nSatellite: ' +sat+ ' \nDownlink: ' +dl['downlink']+ ' \nLines: ' +lines+ '\nPass: ' +passtime+ ' UTC'), timeout=60000)
                        
                        bot.send_document(dataID, fileio, caption=('Satellite: ' +sat+ '\nPass: ' +passtime+ ' UTC \n'), timeout=60000)
                        print("sended", 'index:', a)
                        unproc.pop(a)
                        
                        print("poped",product)
                        a-=1
                        errorCounter=0
                    else:
                        defectCounter+=1
                        print("     defective folder!!! time:",round(defectCounter/6,3),"min")
                        if  defectCounter>=maxTime*6:
                            unproc.pop(a)
                            a-=1
                            print("     poped by time:", round(defectCounter/6), 'index:', a)
                            bot.send_message(logID, (product+' poped by time'))
                            defectCounter=0
                a+=1
    except Exception:
        errorCounter+=1
        print(traceback.format_exc(), "|Count:", errorCounter)
        if  errorCounter>=4:
            bot.send_message(logID, traceback.format_exc()+str('|||| product ||||' + product))
            unproc.pop(a)
            a-=1
            print("  poped by max attempts:", errorCounter, 'index:', a)
            errorCounter=0

while True:
    findFolders()
    time.sleep(10)