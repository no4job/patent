__author__ = 'mishninDY'
import os
import subprocess
import json
import math
import csv
import pprint
import logging
import time
import shutil

class dialect_tab(csv.excel):
    delimiter = '\t'

class dialect_semicolon(csv.excel):
    delimiter = ';'

class csvLog:
    def __init__(self,filename, append = 0,save_intial = 0):
        if save_intial == 1:
            if os.path.exists(filename):
                with open(filename,"r") as self.outFile:
                    self.outCsvDictReader = csv.DictReader(self.outFile, dialect=dialect_tab)
                    self.initialCSV = []
                    for row in self.outCsvDictReader:
                        self.initialCSV.append(row)
                    self.initialCSV = tuple(self.initialCSV)
            else:
                self.initialCSV = ()
        if append == 1:
            self.outFile = open(filename, 'a+', newline='')
        else:
            if os.path.exists(filename):
                os.remove(filename)
            self.outFile = open(filename, 'w+', newline='')
        self.outCsvWriter = csv.writer(self.outFile, delimiter='\t',quoting=csv.QUOTE_MINIMAL)
    def close(self):
        self.outFile.close()
    def __dell__(self):
        self.close()
    def add_row(self,row):
        self.outCsvWriter.writerow(row)
        self.outFile.flush()
    def add_rows(self,rows):
        self.outCsvWriter.writerows(rows)
        self.outFile.flush()
    def getInitialCSV(self):
        return self.initialCSV

# with open (FILE_LIST, 'r') as fileList:
#     fileListReader = csv.DictReader(fileList, dialect=dialect)
#     for row in fileListReader:
#         totalFileSizeOfList += int(row["size"])
#         totalDurationOfList += int(row["duration"])
#         totalNumberOfFiles += 1

def localize_float_row_dict(row):
    out = []
    for value in row:
        if isinstance(value, float):
            out.append(str(value).replace('.', ','))
        else:
            out.append(str(value))
    return out

def localize_float_rows(rows):
    return [
        localize_float_row_dict(row)  for row in rows
        ]
def extract_audio(inputFilePath,outputFilePath,t_start=None,t_finish=None,duration=None):
    try:
        if t_start!= None and t_finish != None:
            duration = t_finish - t_start
        if (duration != None and duration < 0) or (t_start != None and t_start < 0) or  (t_finish != None and t_finish) <0:
            raise ValueError('wrong time parameters extract_audio(inputFilePath,outputFilePath,t_start={},t_finish={},duration={})'.format(t_start,t_finish,duration))
        if t_start==None:
            process = subprocess.Popen(["{}ffmpeg".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error","-y","-i",inputFilePath,"-vn", "-c","copy","-acodec", "pcm_s16le","-ar", "16000",outputFilePath], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        else:
            process = subprocess.Popen(["{}ffmpeg".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error","-y","-i",inputFilePath,"-ss",str(t_start), "-t",str(duration),"-vn", "-c","copy","-acodec", "pcm_s16le","-ar", "16000",outputFilePath], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            # process = subprocess.Popen(["{}ffmpeg".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error","-y","-ss",str(t_start),"-i",inputFilePath, "-t",str(duration),"-c","copy",tmpFilePath], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        process.wait(300)
        # stdout = process.communicate()[0]
        stderr = process.communicate()[1]
        if len(stderr) != 0:
            logger.error(stderr.decode())
    except:
        logger.exception("Exception in total: Something go wrong:")

def seconds_to_time(sec):
    hour = int(sec // 3600)
    minutes = int((sec - sec // 3600)  // 60)
    seconds = sec % 60
    return (hour,minutes,seconds)

# print (os.getcwd())
FFMPEG_DIR="..\\FFMPEG\\BIN\\"
IN_DIR="../IN/"
OUT_DIR="../OUT"
EXTRACTED_WAV_DIR=OUT_DIR+"/extracted_wav"
EXTRACTED_WAV_FRAGMENTS_DIR = OUT_DIR+"/extracted_wav_fragments"
r128_FRAME_DURATION=0.1
SHORT_FRAME_DURATION = 180
SHORT_FRAME_SHIFT_STEP = 15
MIN_LAST_SHIFT_STEP = 5
# CALC_TOTAL = 1
CALC_SHORT_FRAMES = 1
SKIP_FILES_IN_LOG = 1
APPEND_TOTAL_LOG = 1
APPEND_SHORT_FRAMES_LOG = 0
SAVE_INITIAL_TOTAL_LOG = 1
SAVE_INITIAL_SHORT_FRAMES_LOG = 0
EXTRACT_AUDIO_ONLY = 1
EXTRACT_AUDIO_FRAGMENTS = 1

# LONG_FRAME_DURATION = SHORT_FRAME_DURATION*12 # 15*12=180s = 3 min
# SHORT_FRAME_SIZE=SHORT_FRAME_DURATION // r128_FRAME_DURATION
# LONG_FRAME_SIZE=LONG_FRAME_DURATION // r128_FRAME_DURATION
FILE_LIST = IN_DIR+"file_list.csv"

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler(OUT_DIR+'/'+'sound_measure.log')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

logger.info('START with next options: '
            'SHORT_FRAME_DURATION = {} CALC_SHORT_FRAMES={} SKIP_FILES_IN_LOG={} '
            'APPEND_TOTAL_LOG={} APPEND_SHORT_FRAMES_LOG={} SAVE_INITIAL_TOTAL_LOG={} '
            'SAVE_INITIAL_SHORT_FRAMES_LOG={} EXTRACT_AUDIO_ONLY = {}'.format(SHORT_FRAME_DURATION,CALC_SHORT_FRAMES,SKIP_FILES_IN_LOG,
                                                        APPEND_TOTAL_LOG,APPEND_SHORT_FRAMES_LOG,SAVE_INITIAL_TOTAL_LOG,
                                                      SAVE_INITIAL_SHORT_FRAMES_LOG,EXTRACT_AUDIO_ONLY))
shortFrames = []
total=[]
# rootdir = "D:\Полезное видео\GIT"
# rootdir = "H:\Полезное видео\Распознавание речи"
# rootdir = "D:\Полезное видео\Распознавание речи\cs224n 2017"
# rootdir = "\\\\ANDROID_6F6C\share\sda\sda1\cs224n 2017"
# rootdir = "L:\MGS-HNAS-FS-01.mgs.ru"
# rootdir = "H:\\test2"
#***rootdir = "\\\\MGS-HNAS-FS-01.mgs.ru\SAVFSZ_R"
rootdir =""

if EXTRACT_AUDIO_ONLY == 1:
    if not os.path.exists(EXTRACTED_WAV_DIR):
        os.mkdir(EXTRACTED_WAV_DIR)
    else:
        shutil.rmtree(EXTRACTED_WAV_DIR)
        os.mkdir(EXTRACTED_WAV_DIR)
    if EXTRACT_AUDIO_FRAGMENTS == 1:
        if not os.path.exists(EXTRACTED_WAV_FRAGMENTS_DIR):
            os.mkdir(EXTRACTED_WAV_FRAGMENTS_DIR)
        else:
            shutil.rmtree(EXTRACTED_WAV_FRAGMENTS_DIR)
            os.mkdir(EXTRACTED_WAV_FRAGMENTS_DIR)

    with open (FILE_LIST, 'r') as fileList:

        fileListReader = csv.DictReader(fileList, dialect=dialect_tab)
        fileListReader_ = list(fileListReader)
        count = 1
        for row in fileListReader_:
            logger.info('Extract #{}/{} source path:{} '.format(count,len(fileListReader_),row["path"]))
            outFilePath = EXTRACTED_WAV_DIR+'/'+ os.path.splitext(os.path.basename(row["path"]))[0]+".wav"
            extract_audio(os.path.join(rootdir, row["path"]),outFilePath)
            if EXTRACT_AUDIO_FRAGMENTS == 1:
                fragment_exact_start =  "{}h {}m {}.{}s".format(*row["t_exact_start"].split("."))
                fragment_exact_finish = "{}h {}m {}.{}s".format(*row["t_exact_finish"].split("."))
                fragment_cut_start =  row["t_cut_start"].replace(',', '.')
                fragment_cut_finish =  row["t_cut_finish"].replace(',', '.')
                # out.append(str(value).replace('.', ','))
                # d=seconds_to_time(float(fragment_cut_finish)-float(fragment_cut_start))
                # f_duration = "{}h {}m {:.3f}s".format(d[0],d[1],d[2])
                f_duration = "{}h {}m {:.3f}s".format(*seconds_to_time(float(fragment_cut_finish)-float(fragment_cut_start)))
                time_description = "{}({}s) - {}({}s) ~ {}({:.3f}s)".format(fragment_exact_start,fragment_cut_start,fragment_exact_finish,fragment_cut_finish,f_duration,float(fragment_cut_finish)-float(fragment_cut_start))
                outFilePath = EXTRACTED_WAV_FRAGMENTS_DIR+'/'+ os.path.splitext(os.path.basename(row["path"]))[0]+" "+time_description+".wav"
                extract_audio(os.path.join(rootdir, row["path"]),outFilePath,float(fragment_cut_start),float(fragment_cut_finish))
            count+=1
    exit (0)


totalLevelCSV = csvLog(OUT_DIR+'\\sound_level_total.csv',APPEND_TOTAL_LOG,SAVE_INITIAL_TOTAL_LOG)
if not APPEND_TOTAL_LOG:
    totalLevelCSV.add_row(["path","time","size","duration","actual_duration","calc_time_stamp","lavfi_r128_I","lavfi_r128_LRA","lavfi_r128_LRA_low","lavfi_r128_LRA_high","lavfi_r128_true_peaks_ch0",
                           "error","exception"])

shortFrameLevelCSV = csvLog(OUT_DIR+'\\sound_level_frames.csv',APPEND_SHORT_FRAMES_LOG,SAVE_INITIAL_SHORT_FRAMES_LOG)
if not APPEND_SHORT_FRAMES_LOG:
    shortFrameLevelCSV.add_row(["path","time","size","duration","actual_duration","calc_time_stamp","lavfi_r128_I","lavfi_r128_LRA","lavfi_r128_LRA_low","lavfi_r128_LRA_high","lavfi_r128_true_peaks_ch0",
                           "t_start","t_finish","count","error","exception"])

if not os.path.exists(FILE_LIST):
    logger.error("file {} not found".format(FILE_LIST))
    exit(-1)
# for row in totalLevelCSV.getInitialCSV():
#     print (row)

initialCSVFileList = [row["path"]for row in totalLevelCSV.getInitialCSV()]

totalFileSizeOfList = 0
totalDurationOfList = 0
totalNumberOfFiles = 0
fileCount = 0
totalCalcTime =0
sizeOfProcessedFiles=0
durationOfProcessedFiles=0
skipped = 0
inputFilePathSave = ""
total_count = 0
with open (FILE_LIST, 'r') as fileList:
    fileListReader = csv.DictReader(fileList, dialect=dialect_tab)
    for row in fileListReader:
        if os.path.join(rootdir, row["path"]) in initialCSVFileList:
            if CALC_SHORT_FRAMES == 0:
                skipped +=1
                continue
            else:
                skipped +=1
        totalFileSizeOfList += int(row["size"])
        totalDurationOfList += int(row["duration"])
        totalNumberOfFiles += 1
if CALC_SHORT_FRAMES == 0:
    logger.info('list contains {} files for total calc, total calc. skipped for {} files, total calc. left: {} files, '
                'total size {:.1f}Mb, total duration {:.1f}мин'
                .format(totalNumberOfFiles+skipped,skipped,totalNumberOfFiles,
                        totalFileSizeOfList/1000000,totalDurationOfList/60))
else:
    logger.info('list contains {} files for total and short frames calc, total calc. skipped for {} files, '
                'left (total/short frames){}/{} files, total size {:.1f}Mb, total duration {:.1f}мин'
                .format(totalNumberOfFiles,skipped,totalNumberOfFiles-skipped,totalNumberOfFiles,
                        totalFileSizeOfList/1000000,totalDurationOfList/60))
per_file_t_start = time.clock()



with open (FILE_LIST, 'r') as fileList:
    fileListReader = csv.DictReader(fileList, dialect=dialect_tab)
    for row in fileListReader:
        total=[]
        shortFrames = []
        totalCalcError = 0
        # recalc = int(row.get("recalc", "0"))
        # calcTotal = int(row.get("calc_total", "0"))
        # calcShortFrames = int(row.get("short", "0"))
        if os.path.join(rootdir, row["path"]) in initialCSVFileList and CALC_SHORT_FRAMES == 0:
            continue
        fileCount+=1
        timeStamp = float(row["time"].replace(',', '.'))
        file_size = int(row["size"])
        duration = int(row["duration"])
        path = row["path"]
        inputFilePath = os.path.join(rootdir, path)
        inputFilePathSave = inputFilePath
        sizeOfProcessedFiles += file_size
        durationOfProcessedFiles += duration
# for subdir, dirs, files in os.walk(rootdir):
#     for file in files:
#         filename, file_extension = os.path.splitext(file)
#         if  file_extension.lower() != ".mp4":
#             continue
#         inputFilePath=os.path.join(subdir, file)
#         logger.info('file: {}'.format(inputFilePath))
        logger.info('#{}/{}\t{:.1f}min\t{:.1f}Mb\tpath: {}'.format(fileCount,totalNumberOfFiles,duration/60,file_size/1000000, inputFilePath))
        #print(inputFilePath.encode('cp866', 'ignore'))

        # inputFilePath=inputFilePath.replace(r"\\",r"/")
        # inputFilePath=inputFilePath.replace(":",r"\\\\:")
        #inputFilePath="../IN/test.wav"
        # tmpFilePath=OUT_DIR+"/tmp.mkv"
        tmpFilePath=OUT_DIR+"/tmp.mp4"
        # tmpFilePath=OUT_DIR+"/tmp.wav"
        wavFilePath=OUT_DIR+"/inp.mp4"
        ffprobeLavfiStr="amovie={},ebur128=metadata=1:peak=true".format(inputFilePath.replace("\\",r"/").replace(":",r"\\:"))
        ffprobeLavfiStrTmp="amovie={},ebur128=metadata=1:peak=true".format(tmpFilePath.replace("\\",r"/").replace(":",r"\\:"))
        ffmpegFilterStr="ebur128=metadata=1:peak=true"
        try:
            # process = subprocess.Popen(["{}ffmpeg".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error","-y","-i",inputFilePath,"-vn", "-c","copy",tmpFilePath], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            process = subprocess.Popen(["{}ffmpeg".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error","-y","-i",inputFilePath,"-vn", "-c","copy",wavFilePath], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            process.wait(300)
            stdout = process.communicate()[0]
            stderr = process.communicate()[1]
            if len(stderr) != 0:
                totalCalcError = 1
                logger.error(stderr.decode())
            # inputFilePathSave = inputFilePath
            inputFilePath = wavFilePath
#************************************************
            if os.path.join(rootdir, row["path"]) not in initialCSVFileList or CALC_SHORT_FRAMES == 1:
                # process = subprocess.Popen(["{}ffprobe".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error",inputFilePath+"rrr" ,"-show_format", "-print_format", "json"], stdout=subprocess.PIPE)
                process = subprocess.Popen(["{}ffprobe".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error",inputFilePath,"-show_format", "-print_format", "json"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                stdout = process.communicate()[0]
                stderr = process.communicate()[1]
                # print ('STDOUT:{}'.format(stdout))
                # print ('STDOUT:{} \n STDERR:{}'.format(stdout,stderr))
                if len(stderr) != 0:
                    totalCalcError = 1
                    logger.error(stderr.decode())
                parsed =  json.loads(stdout)
                totalActualDuration=float(parsed["format"]["duration"])
                if CALC_SHORT_FRAMES == 0:
                    process = subprocess.Popen(["{}ffprobe".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error", "-f", "lavfi","-i", ffprobeLavfiStr, "-show_frames","-show_format", "-print_format", "json"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = process.communicate()[0]
                    stderr = process.communicate()[1]
                    if len(stderr) != 0:
                        totalCalcError = 1
                        logger.error(stderr.decode())
                    # print ('STDOUT:{}'.format(stdout))
                    parsed =  json.loads(stdout)
                    process.wait(900)
                    # total=[]
                    for frame in reversed(parsed["frames"]):
                        if "tags" in frame:
                            lavfi_r128_I_total = float(frame["tags"]["lavfi.r128.I"])
                            lavfi_r128_LRA_total = float(frame["tags"]["lavfi.r128.LRA"])
                            lavfi_r128_LRA_low_total = float(frame["tags"]["lavfi.r128.LRA.low"])
                            lavfi_r128_LRA_high_total = float(frame["tags"]["lavfi.r128.LRA.high"])
                            lavfi_r128_true_peaks_ch0_total = float(frame["tags"]["lavfi.r128.true_peaks_ch0"])
                            # lavfi_r128_true_peaks_ch1_total = float(frame["tags"]["lavfi.r128.true_peaks_ch1"])
                            # total.append([lavfi_r128_I_total,lavfi_r128_LRA_total,lavfi_r128_LRA_low_total,lavfi_r128_LRA_high_total,lavfi_r128_true_peaks_ch0_total,lavfi_r128_true_peaks_ch1_total,
                            #               totalDuration,inputFilePath])
                            total.append([inputFilePathSave,timeStamp,file_size,duration,totalActualDuration,time.clock(),lavfi_r128_I_total,lavfi_r128_LRA_total,
                                          lavfi_r128_LRA_low_total,lavfi_r128_LRA_high_total,lavfi_r128_true_peaks_ch0_total,
                                          totalCalcError,0])
                            break
                    # totalLevelCSV.add_rows(total)

 #******************************
            t_start = 0
            if totalActualDuration - (t_start +  SHORT_FRAME_DURATION) < MIN_LAST_SHIFT_STEP:
                t_finish = totalActualDuration
            else:
                t_finish = t_start +  SHORT_FRAME_DURATION
            count = 0
            ffmpegTime=0
            ffprobeTime=0
            shortFrameCalcError = 0
            # if SHORT_FRAME_DURATION*(((totalActualDuration/SHORT_FRAME_DURATION)) - ((totalActualDuration/SHORT_FRAME_DURATION)//1)) >= MIN_LAST_FRAME:
            if SHORT_FRAME_SHIFT_STEP*(((totalActualDuration-SHORT_FRAME_DURATION)/SHORT_FRAME_SHIFT_STEP) - (((totalActualDuration-SHORT_FRAME_DURATION)/SHORT_FRAME_SHIFT_STEP)//1)) >= MIN_LAST_SHIFT_STEP:
                # total_count= int((totalActualDuration/SHORT_FRAME_DURATION)//1) + 1
                # total_count= int(((totalActualDuration-SHORT_FRAME_SHIFT_STEP)/SHORT_FRAME_SHIFT_STEP)//1) + 1
                total_count= int(((totalActualDuration-SHORT_FRAME_DURATION)/SHORT_FRAME_SHIFT_STEP)//1) + 2
            else:
                # total_count= int(((totalActualDuration-SHORT_FRAME_SHIFT_STEP)/SHORT_FRAME_SHIFT_STEP)//1)
                total_count= int(((totalActualDuration-SHORT_FRAME_DURATION)/SHORT_FRAME_SHIFT_STEP)//1) + 1
            try:
                while  t_finish <= totalActualDuration and count < total_count and CALC_SHORT_FRAMES == 1:
                    t_ = time.clock()
                    #process = subprocess.Popen(["{}ffmpeg".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error","-y","-i",inputFilePath,"-vn", "-ss",str(t_start),"-t",str(SHORT_FRAME_DURATION),"-c","copy",tmpFilePath], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    #process = subprocess.Popen(["{}ffmpeg".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error","-y","-i",inputFilePath, "-ss",str(t_start),"-t",str(SHORT_FRAME_DURATION),"-c","copy",tmpFilePath], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    #process = subprocess.Popen(["{}ffmpeg".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error","-y","-ss",str(t_start),"-i",inputFilePath,"-vn", "-t",str(SHORT_FRAME_DURATION),"-c","copy",tmpFilePath], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    process = subprocess.Popen(["{}ffmpeg".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error","-y","-ss",str(t_start),"-i",inputFilePath, "-t",str(SHORT_FRAME_DURATION),"-c","copy",tmpFilePath], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    process.wait(300)
                    ffmpegTime += time.clock() - t_
                    stdout = process.communicate()[0]
                    stderr = process.communicate()[1]
                    if len(stderr) != 0:
                        shortFrameCalcError = 1
                        logger.error(stderr.decode())
                    # print("ffmpeg:{}".format(process.wait()))
                    t_ = time.clock()
                    process = subprocess.Popen(["{}ffprobe".format(FFMPEG_DIR),"-hide_banner", "-loglevel", "error", "-f", "lavfi","-i", ffprobeLavfiStrTmp,"-show_frames", "-show_format", "-print_format", "json"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    stdout = process.communicate()[0]
                    stderr = process.communicate()[1]
                    if len(stderr) != 0:
                        shortFrameCalcError = 1
                        logger.error(stderr.decode())
                    parsed =  json.loads(stdout)
                    process.wait(900)
                    ffprobeTime += time.clock() - t_
                    # print("ffprobe:{}".format(process.wait()))
                    for frame in reversed(parsed["frames"]):
                        if "tags" in frame:
                            lavfi_r128_I = float(frame["tags"]["lavfi.r128.I"])
                            lavfi_r128_LRA = float(frame["tags"]["lavfi.r128.LRA"])
                            lavfi_r128_LRA_low = float(frame["tags"]["lavfi.r128.LRA.low"])
                            lavfi_r128_LRA_high = float(frame["tags"]["lavfi.r128.LRA.high"])
                            lavfi_r128_true_peaks_ch0 = float(frame["tags"]["lavfi.r128.true_peaks_ch0"])
                            # lavfi_r128_true_peaks_ch1 = float(frame["tags"]["lavfi.r128.true_peaks_ch1"])
                            # shortFrames.append([lavfi_r128_I,lavfi_r128_LRA,lavfi_r128_LRA_low,lavfi_r128_LRA_high,lavfi_r128_true_peaks_ch0,lavfi_r128_true_peaks_ch1,
                            #                     t_start,t_finish,count,inputFilePath])
                            shortFrames.append([inputFilePathSave,timeStamp,file_size,duration,totalActualDuration,
                                                time.clock(),lavfi_r128_I,lavfi_r128_LRA,lavfi_r128_LRA_low,lavfi_r128_LRA_high,lavfi_r128_true_peaks_ch0,
                                                t_start,t_finish,count,shortFrameCalcError,0])
                            break
                    # t_start = t_finish
                    t_start += SHORT_FRAME_SHIFT_STEP
                    # t_finish = t_start +  SHORT_FRAME_DURATION
                    count+=1
                    if count+1 > total_count:
                        break
                    elif count+1 == total_count:
                        t_finish = totalActualDuration
                        t_start = t_finish - SHORT_FRAME_DURATION
                    else:
                        t_finish = t_start +  SHORT_FRAME_DURATION
                        # t_finish = t_start +  SHORT_FRAME_SHIFT_STEP



                    # if totalActualDuration - (t_start +  SHORT_FRAME_DURATION) < MIN_LAST_FRAME:
                    #     t_finish = totalActualDuration
                    # else:
                    #     t_finish = t_start +  SHORT_FRAME_DURATION

                    # print(count,t_start , t_finish)
                if CALC_SHORT_FRAMES == 1:
                    logger.debug('ffmpeg/ffprobe execution time : {}'.format(ffmpegTime/ffprobeTime))

            except:
                shortFrames.append([inputFilePathSave,timeStamp,file_size,duration,"",
                                    "","","","","",t_start,t_finish,count,1,1])

                logger.exception("Exception in short frame #{}/{}".format(count,total_count))

#*********************************************

            totalCalcTime = time.clock()
            per_file_t_finish = totalCalcTime
            calcTime = per_file_t_finish - per_file_t_start
            per_file_t_start = time.clock()
            # print("per_file_t_finish={} per_file_t_start={}".format(per_file_t_finish, per_file_t_start))
            # remainTime = totalCalcTime*(totalFileSizeOfList/sizeOfProcessedFiles -1)
            remainTime = totalCalcTime*(totalDurationOfList/durationOfProcessedFiles -1)
            if CALC_SHORT_FRAMES == 0:
                logger.info('calc. type: total\tcalc. time {:.1f}min\ttotal calc. time {:.1f}min\tremain {:.1f}min'.format(calcTime/60,totalCalcTime/60,remainTime/60))
            else:
                logger.info('calc. type: total and short frames, number of short frames ({})\tcalc. time {:.1f}min\ttotal calc. time {:.1f}min\tremain {:.1f}min'.format(total_count ,calcTime/60,totalCalcTime/60,remainTime/60))
            # totalLevelCSV.add_rows(total)
            totalLevelCSV.add_rows(localize_float_rows(total))
            # shortFrameLevelCSV.add_rows(shortFrames)
            if CALC_SHORT_FRAMES == 1:
                shortFrameLevelCSV.add_rows(localize_float_rows(shortFrames))
        except:
            total.append([inputFilePathSave,timeStamp,file_size,duration,"","","",
                          "","","", 1,1])
            totalLevelCSV.add_rows(localize_float_rows(total))
            logger.exception("Exception in total: Something go wrong:")

shortFrameLevelCSV.close()
totalLevelCSV.close()
exit(0)




