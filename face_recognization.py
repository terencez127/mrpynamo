import worker
from collections import Counter
import cPickle as pickle
import os
from PIL import Image, ImageDraw
import cv
import numpy
import cv2
from cStringIO import StringIO
import numpy as np

class MyMapper(worker.Mapper):
    def detect_object(self, image):
        grayscale = cv.CreateImage((image.width, image.height), 8, 1)
        cv.CvtColor(image, grayscale, cv.CV_BGR2GRAY)

        cascade = cv.Load("/usr/share/opencv/haarcascades/haarcascade_frontalface_alt_tree.xml")
        rect = cv.HaarDetectObjects(grayscale, cascade, cv.CreateMemStorage(), 1.1, 2,
                                    cv.CV_HAAR_DO_CANNY_PRUNING, (20, 20))

        result = []
        for r in rect:
            result.append((r[0][0], r[0][1], r[0][0] + r[0][2], r[0][1] + r[0][3]))

        return result


    def process(self, img_data):
        s = StringIO(img_data)
        img_array = np.asarray(bytearray(s.read()), dtype=np.uint8)
        image = cv.fromarray(cv2.imdecode(img_array, cv2.CV_LOAD_IMAGE_COLOR))
        print len(img_data)
        print type(image)
        
        if image:
            faces = self.detect_object(image)
        io = StringIO(img_data)
        im = Image.open(io)
        result = []
        if faces:
            draw = ImageDraw.Draw(im)
            count = 0
            for f in faces:
                count += 1
                draw.rectangle(f, outline=(255, 0, 0))
                a = im.crop(f)
                output = StringIO()
                a.save(output, format="JPEG", quality=100)
                content = output.getvalue()
                output.close()
                result.append(content)

        else:
            print "Error: cannot detect faces on %s" % img_data

        io.close()

        return result

    def map(self, data):
        ret = self.process(data)
        return ret


class MyReducer(worker.Reducer):
    def __init__(self, dynamo):
        worker.Reducer.__init__(self, dynamo)
        self.result = []
        self.raw_sample = self.dynamo_client.get('sample.jpg')[0]
        self.sample = None

    def reduce(self, data):
        if self.sample is None:
            d = StringIO(self.raw_sample)
            im = Image.open(d)
            self.sample = numpy.array(im)
            # Convert RGB to BGR
            self.sample = self.sample[:, :, ::-1].copy()
            self.sample = cv2.cvtColor(self.sample, cv2.COLOR_BGR2GRAY)
            self.raw_sample = None

        for img in data:
            d = StringIO(img)
            im = Image.open(d)
            new_img = numpy.array(im)
            # Convert RGB to BGR
            new_img = new_img[:, :, ::-1].copy()

            ngrey = cv2.cvtColor(new_img, cv2.COLOR_BGR2GRAY)
            # hgrey = cv2.cvtColor(self.sample, cv2.COLOR_BGR2GRAY)
            hgrey = self.sample

            # build feature detector and descriptor extractor
            hessian_threshold = 85
            detector = cv2.SURF(hessian_threshold)
            (hkeypoints, hdescriptors) = detector.detect(hgrey, None, useProvidedKeypoints=False)
            (nkeypoints, ndescriptors) = detector.detect(ngrey, None, useProvidedKeypoints=False)

            # extract vectors of size 64 from raw descriptors numpy arrays
            rowsize = len(hdescriptors) / len(hkeypoints)
            if rowsize > 1:
                hrows = numpy.array(hdescriptors, dtype=numpy.float32).reshape((-1, rowsize))
                nrows = numpy.array(ndescriptors, dtype=numpy.float32).reshape((-1, rowsize))
                #print hrows.shape, nrows.shape
            else:
                hrows = numpy.array(hdescriptors, dtype=numpy.float32)
                nrows = numpy.array(ndescriptors, dtype=numpy.float32)
                rowsize = len(hrows[0])

            # kNN training - learn mapping from hrow to hkeypoints index
            samples = hrows
            responses = numpy.arange(len(hkeypoints), dtype=numpy.float32)
            #print len(samples), len(responses)
            knn = cv2.KNearest()
            knn.train(samples, responses)

            # retrieve index and value through enumeration
            count = 0

            for i, descriptor in enumerate(nrows):
                descriptor = numpy.array(descriptor, dtype=numpy.float32).reshape((1, rowsize))
                retval, results, neigh_resp, dists = knn.find_nearest(descriptor, 1)
                res, dist = int(results[0][0]), dists[0][0]

                if dist < 0.1:
                    count += 1

            if len(nrows) > 0:
                print 'rate: ', count*1.0/len(nrows)
                if count*1.0/len(nrows) > 0.53:
                    self.result.append(img)
                # with open(str(i)+'.jpg', 'w+') as f:
                #     f. write(img)
    def output(self):
        return pickle.dumps(self.result)