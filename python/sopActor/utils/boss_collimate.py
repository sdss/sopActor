"""
Computes spectrograph collimation focus from Hartmann mask exposures.

Replacement for idlspec2d combsmallcollimate+sosActor.
SOP shouldn't have a dependency on SoS, so the code for the short Hartmanns
is here now.

Example:
    # cmd is the current Command instance, for message passing.
    # take Hartmann exposures and output collimation values:
    hartmann = Hartmann()
    hartmann.doHartmann(cmd)
    # solve for the focus of exposure numbers 12345,12346 on the current MJD
    hartmann.collimate(cmd,12345)

The focus of the collimator is measured by comparing two Hartmann
exposures of arc lamps, and looking for shifts in the arc line
positions. A linear correlation coefficient is computed independently
in a subregion on each CCD as a function of pixel shifts of the 2nd
image in both X and Y.  The best-focus value is found in each region
by maximizing the linear correlation in the Y (wavelength) direction.

The position of the Hartmann shutters is read from the OBSCOMM (for
SDSS-I: '{focus, hartmann l}' or '{focus, hartmann l}') or HARTMANN
(for BOSS: 'Left' or 'Right') header keywords for SDSS-I.  It is
expected to be '{focus, hartmann l}' for one exposure and '{focus,
hartmann r}' for the other (in either order). It is assumed that the
collimator position is identical for both exposures.

The sense of the pixel shifts reported is what one would have to shift
the Hartmann-r exposure by in Y to agree with the Hartmann-l exposure.
"""
# IDL version by Kyle Dawson, David Schlegel, Matt Olmstead
# IDL->Python verson by John Parejko

#import sopActor.myGlobals as myGlobals

import logging
import os.path
import glob

import pyfits
import numpy as np
from scipy.ndimage import interpolation
import matplotlib.pyplot as plt

class HartError(Exception):
    """For known errors processing the Hartmanns"""
    pass
#...

class Hartmann(object):
    """
    Call Hartmann.doHartmann to take and reduce a pair of hartmann exposures.
    """
    def __init__(self):
        # final results go here
        self.result = {'sp1':{'b':0.,'r':0.},'sp2':{'b':0.,'r':0.}}
        self.success = True

        self.data_root_dir = '/data/spectro'
        self.cam_gains = {'b1':[1.048, 1.048, 1.018, 1.006], 'b2':[1.040, 0.994, 1.002, 1.010],
                          'r1':[1.966, 1.566, 1.542, 1.546], 'r2':[1.598, 1.656, 1.582, 1.594]}
        self.gain_slice = {'b':[[np.s_[0:2055,0:2047],np.s_[56:2111,128:2175]],
                                [np.s_[0:2055,2048:4095],np.s_[56:2111,2176:4223]]],
                           'r':[[np.s_[0:2063,0:2056],np.s_[48:2111,119:2175]],
                                [np.s_[0:2063,2057:4113],np.s_[48:2111,2176:4232]]],}
        self.bias_slice = [np.s_[950:1338,10:100],np.s_[950:1338,4250:4340]]
        self.filebase = 'Collimate-%5d-%s-%08d'
        # These are unused at present
        self.plotfilebase = self.filebase+'.png'
        #self.logfilebase = self.filebase+'.log'

        # allowable focus tolerance (pixels): if offset is less than this, we're in focus.
        self.focustol = 0.20
        # bad residual on blue ring
        self.badres = 6
        
        # maximum pixel shift to search in X
        self.maxshift = 2
        
        # region to use in the analysis. [xlow,xhigh,ylow,yhigh]
        # NOTE: this region is chosen to have no blending and strong lines.
        # Even though there are only two lines in the blue, there should be enough signal
        # in one line to get a good cross-correlation.
        # Also, the other blue lines have a higher temperature dependence, so 
        # we don't want to use them, as the arc lamp might not be warm yet.
        self.region = np.s_[850:1301,1500:2501]
        
        # collimator motion constants for the different regions.
        # TBD: these values should be loaded into a table/platedb
        # (or something, so modifying them is easier, and we can keep track of changes)
        self.m = {'b1':1.,'b2':1.,'r1':1.,'r2':1.}
        self.b = {'b1':0.066,'b2':-0.00,'r1':-0.31,'r2':0.024}
        
        # "funny fudge factors" from Kyle Dawson
        # TBD: The "funny fudge factors" can be turned into a single number, and
        # there's no really good reason to not just make them one constant.
        # These values were intially determined by comparing to the original SDSS spectrographs
        # They need to be adjusted when, e.g., the spectrograph motors are changed.
        # pixscale = -15. vs. pixscale/24. is because of the change from SDSS to BOSS
        pixscale = -15. # in microns
        rfudge = -9150*1.12*pixscale/24.
        # steps per degree for the blue ring.
        self.bsteps = 292.
        self.fudge = {'b1':-31.87*self.bsteps*pixscale/24.,
                      'b2':-28.95*self.bsteps*pixscale/24.,
                      'r1':rfudge,'r2':rfudge}
    #...
    
    def check_Hartmann_header(self,header):
        """
        Return a code signifying whether this is a left (-1), right (1) or unknown (0) Hartmann.
        """
        # OBSCOMM only exists in dithered flats taken with "specFlats", so 
        # we have to "get" it (returns None if missing), not just take it as a keyword.
        obscomm = header.get('OBSCOMM')
        hartmann = header.get('HARTMANN')
        if obscomm == '{focus, hartmann l}' or hartmann == 'Left':
            return -1
        elif obscomm == '{focus, hartmann r}' or hartmann == 'Right':
            return 1
        else:
            return 0
    #...
    
    def bad_header(self,header):
        """
        Return True if the header indicates there are no lamps on or no flat-field
        petals closed.
        If the wrong number of petals are closed, then emit a warning and return False.
        """
        sum_string = lambda field: sum([int(x) for x in field.split()])
        
        isBad = False
        ffs = header.get('FFS',None)
        # 8 flat field screens: 0 for open, 1 for closed
        if ffs:
            ffs_sum = sum_string(ffs)
            if ffs_sum < 8:
                self.cmd.warn("text='Only %d of 8 flat-field petals closed: %s'"%(ffs_sum,ffs))
            if ffs_sum == 0:
                isBad = True
        else:
            self.cmd.warn("text='FFS not in FITS header!'")
            isBad = True
        
        Ne = header.get('NE',None)
        HgCd = header.get('HGCD',None)
        # 4 of each lamp: 0 for off, 1 for on
        if Ne is not None and HgCd is not None:
            Ne_sum = sum_string(Ne)
            HgCd_sum = sum_string(HgCd)
            if Ne_sum < 4:
                self.cmd.warn("text='Only %d of 4 Ne lamps turned on: %s'"%(Ne_sum,Ne))
            if HgCd_sum < 4:
                self.cmd.warn("text='Only %d of 4 HgCd lamps turned on: %s'"%(HgCd_sum,HgCd))
            if Ne_sum + HgCd_sum == 0:
                isBad = True
        else:
            self.cmd.warn("text='NE and/or HgCd not in FITS header.'")
            isBad = True
        
        return isBad
    #...
    
    def do_one_cam(self,cam,indir,basename,expnum1,expnum2,test,plot):
        """
        Compute the collimation values for one camera.
        
        See parameters for self.collimate().
        """
        self.cam = cam
        self.test = test
        try:
            self._load_data(indir,basename,expnum1,expnum2)
            self._do_gain_bias()
            self._check_images()
            self._find_shift()
            self._find_collimator_motion()
        except HartError,e:
            self.cmd.error('text="%s"'%e)
            raise
        except Exception,e:
            self.cmd.error('text="!!!! Unknown error when processing Hartmanns! !!!!"')
            self.cmd.error('text="%s"'%e)
            raise
        else:
            if plot:
                self._make_plot(cam,expnum1,expnum2)
    
    def _make_plot(self,cam,expnum1,expnum2):
        """Save a plot of the pixel vs. correlation for this camera."""
        ylim1 = [0.4,1.05]
        ylim2 = [0.92,1.01]
        inset_xlim = 14
        inset = [self.ibest-inset_xlim,self.ibest+inset_xlim]
        # prevent array overflow when getting the inset plot range.
        if self.ibest < inset_xlim:
            inset[0] = 0
        if len(self.xshift)-self.ibest < inset_xlim:
            inset[1] = len(self.xshift)-1
        mjd = self.header1['MJD']
        plotfile = self.plotfilebase%(mjd,cam,expnum1)
        title = 'Collimate: MJD=%5i Camera=%s Exp=%08i-%08i'%(mjd,cam,expnum1,expnum2)
        fig = plt.figure()
        ax1 = fig.add_axes([0.1,0.1,0.8,0.9])
        ax2 = fig.add_axes([0.35,0.2,0.3,0.3])
        ax1.plot(self.xshift,self.coeff/max(self.coeff),'*-',color='black',lw=2)
        ax2.plot(self.xshift[inset[0]:inset[1]],self.coeff[inset[0]:inset[1]]/max(self.coeff),'*-',color='black',lw=2)
        ax1.plot([self.xoffset,self.xoffset],ylim1,'--',color='green')
        ax2.plot([self.xoffset,self.xoffset],ylim2,'--',color='green')
        ax1.set_ylim(ylim1[0],ylim1[1])
        ax2.axis([self.xshift[inset[0]],self.xshift[inset[1]],ylim2[0],ylim2[1]])
        ax1.set_title(title)
        ax1.set_xlabel('pixels')
        ax1.set_ylabel('cross-correlation')
        plt.savefig(plotfile)
    
    def _load_data(self,indir,basename,expnum1,expnum2):
        """
        Read in the two images, and check that headers are reasonable.
        Sets self.bigimg[1,2] and returns True if everything is ok, else return False."""
        filename1path = os.path.join(indir,basename%(self.cam,expnum1))
        filename2path = os.path.join(indir,basename%(self.cam,expnum2))
        try:
            filename1 = glob.glob(filename1path)[0]
            filename2 = glob.glob(filename2path)[0]
        except IndexError:
            raise HartError("All files not found: %s, %s!"%(filename1path,filename2path))
        
        # NOTE: we don't process with sdssproc, because the subarrays cause problems there.
        # Also, because it would restore the dependency on SoS that this class removed!
        try:
            bigimg1,header1 = pyfits.getdata(filename1,0,header=True)
        except IOError:
            raise HartError("Failure reading file %s"%filename1)
        try:
            bigimg2,header2 = pyfits.getdata(filename2,0,header=True)
        except IOError:
            raise HartError("Failure reading file %s"%filename2)

        if self.bad_header(header1) or self.bad_header(header2):
            raise HartError("Incorrect header values in fits file.")
       
        self.hartpos1 = self.check_Hartmann_header(header1)
        self.hartpos2 = self.check_Hartmann_header(header2)
        if self.hartpos1 == 0 or self.hartpos2 == 0:
            raise HartError("FITS headers do not indicate these are Hartmann exposures.")
        if self.hartpos1 == self.hartpos2:
            raise HartError("FITS headers indicate both exposures had same Hartmann position.")

        # upcast the arrays, to make later math work better.
        self.bigimg1 = np.array(bigimg1,dtype='float64')
        self.bigimg2 = np.array(bigimg2,dtype='float64')
        self.header1 = header1
        self.header2 = header2
    #...

    def _do_gain_bias(self):
        """Apply the bias and gain to the images."""
        bigimg1 = self.bigimg1
        bigimg2 = self.bigimg2
        # determine bias levels
        bias1 = [ np.median(bigimg1[self.bias_slice[0]]), np.median(bigimg1[self.bias_slice[1]]) ]
        bias2 = [ np.median(bigimg2[self.bias_slice[0]]), np.median(bigimg2[self.bias_slice[1]]) ]
        # apply bias and gain to the images
        # gain_slice is a dict with keys 'r' and 'b
        try:
            gain = self.cam_gains[self.cam]
            gslice = self.gain_slice[self.cam[0]]
        except KeyError, e:
            raise HartError("I do not recognize camera %s"%self.cam)
        # Only apply gain to quadrants 0 and 1, since we aren't using quadrants 2 and 3.
        # NOTE: we are overwriting the original array, including the original bias region.
        # This is ok, because all the processing is done on a subregion of this,
        # indexed from the new edge.
        bigimg1[gslice[0][0]] = gain[0]*(bigimg1[gslice[0][1]]-bias1[0])
        bigimg1[gslice[1][0]] = gain[1]*(bigimg1[gslice[1][1]]-bias1[1])
        bigimg2[gslice[0][0]] = gain[0]*(bigimg2[gslice[0][1]]-bias2[0])
        bigimg2[gslice[1][0]] = gain[1]*(bigimg2[gslice[1][1]]-bias2[1])
    #...

    def _check_images(self):
        """Check that there is actually light in the images."""
        img1 = self.bigimg1[self.region]
        # find the variance near bright lines
        # ddof=1 for consistency with IDL's variance() which has denominator (N-1)
        if 'b' in self.cam:
            var = np.var(img1[300:450,:],ddof=1)
        else:
            var = np.var(img1[0:150,:],ddof=1)
       # check that the camera is capturing light by requiring variance greater than 100
        if var < 100:
            raise HartError("THERE DOES NOT APPEAR TO BE ANY LIGHT FROM THE ARCS IN %s!!!"%self.cam)
    #...

    def _find_shift(self,order=3):
        """
        Find the best shift between image1 and image2.
        Expects _load_data() to have been run first to initialize things.
        order is the order of the spline used for shifting in the correlation step.
        """
        # Mask pixels around the edges of the image.
        # We don't have inverse-variances because sdssproc isn't run, so we can't do
        # smarter masking, like bad pixel smoothing, cosmic rays, etc.
        # It is important to mask out the ends of the array, because of
        # spline-induced oddities there.
        subimg1 = self.bigimg1[self.region].copy()
        subimg2 = self.bigimg2[self.region].copy()
        mask = np.ones(subimg1.shape,dtype='f8')
        mask[:10,:] = 0
        mask[-10:,:] = 0
        mask[:,:10] = 0
        mask[:,-10:] = 0

        # Compute linear correlation coefficients
        # Calculate the maximum product of the two images when shifting by steps of dx.
        dx = 0.05
        nshift = int(np.ceil(2*self.maxshift/dx))
        xshift = -self.maxshift + dx * np.arange(nshift,dtype='f8')
        self.xshift = xshift # save for plotting
        
        self.coeff = np.zeros(nshift,dtype='f8')
        filtered1 = interpolation.spline_filter(subimg1)
        filtered2 = interpolation.spline_filter(subimg2)
        for i in range(nshift):
            self.coeff[i] = (subimg1*interpolation.shift(subimg2,[xshift[i],0],order=order)*mask).sum()

        ibest = self.coeff.argmax()
        self.ibest = ibest # save for plotting
        self.xoffset = xshift[ibest]
        # If the sequence is actually R-L, instead of L-R, 
        # then the offset acctually goes the other way.
        if self.hartpos1 > self.hartpos2:
            self.xoffset = -self.xoffset
    #...

    def _find_collimator_motion(self):
        """
        Compute the required collimator movement from self.xoffset.
        Assumes _find_shift has been run successfully.

        Current procedure for determining the offsets:
            When the observers start complaining about focus warnings...
            * We have them step through focus between -10000 and 10000,
              taking a full and quick hartmann +flat and arc at each step.
            * Then we have enough information to recalibrate the relationship
              between the full and quick hartmanns.
        This has to happen once every three months or so.
        """
        m = self.m[self.cam]
        b = self.b[self.cam]
        if self.test:
            b = 0.
        offset = self.xoffset*m + b

        if offset < self.focustol:
            focus = 'In Focus'
            msglvl = self.cmd.inform
        else:
            focus = 'Out of focus'
            msglvl = self.cmd.warn
        msglvl('%sMeanOffset=%.2f,"%s"'%(self.cam,offset,focus))

        val = int(offset*self.fudge[self.cam])
        self.result[self.spec][self.cam[0]] = val
        if 'r' in self.cam:
            self.cmd.inform('%sPistonMove=%d'%(self.cam,val))
        else:
            self.cmd.inform('%sRingMove=%.1f'%(self.cam,-val/self.bsteps))
    #...

    def _mean_moves(self):
        """Compute the mean movement and residuals for this spectrograph,
        after r&b moves have been determined."""
        avg = sum(self.result[self.spec].values())/2.
        bres = -(self.result[self.spec]['b'] - avg)/self.bsteps
        rres = self.result[self.spec]['r'] - avg

        if abs(bres) < self.badres:
            resid = '"OK"'
            msglvl = self.cmd.inform
        else:
            resid = '"Bad angle !!!!!!! move blue ring %.1f degrees then run gotoField noSlew"'%(bres*2)
            msglvl = self.cmd.warn
        msglvl('%sResiduals=%d,%.1f,%s'%(self.spec,rres,bres,resid))
        self.cmd.inform('%sAverageMove=%d'%(self.spec,avg))
    #...
    
    def collimate(self,cmd,expnum1,expnum2=None,indir=None,
                  spec=['sp1','sp2'],docams1=['b1','r1'],docams2=['b2','r2'],
                  test=False,plot=False):
        """
        Compute the spectrograph collimation focus from Hartmann mask exposures.
        
        cmd:     command handler
        expnum1: first exposure number of raw sdR file (integer).
        expnum2: second exposure number (default: expnum1+1)
        indir:   directory where the exposures are located.
        spec:    spectrograph(s) to collimate ('sp1','sp2',['sp1','sp2'])
        docams1: camera(s) in sp1 to collimate ('b1','r1',['b1','r1'])
        docams2: camera(s) in sp2 to collimate ('b2','r2',['b2','r2'])
        test:    If True, we are trying to determine the collimation parameters, so ignore 'b' parameter.
        plot:    If True, save a plot of the 
        """
        self.cmd = cmd
        # clear previous collimator movement values
        self.result = {'sp1':{'b':0.,'r':0.},'sp2':{'b':0.,'r':0.}}
        if expnum2 is None:
            expnum2 = expnum1+1
    
        # recursive call for each spectrograph
        if not isinstance(spec,str):
            for sp in spec:
                self.collimate(cmd,expnum1,expnum2=expnum2,indir=indir,spec=sp,
                               docams1=docams1,docams2=docams2,test=test,plot=plot)
            return

        self.spec = spec
        if indir is None:
            indir = os.path.join(self.data_root_dir,'*')

        # to handle the various string/list/tuple possibilities for each argument
        docams = []
        if spec == 'sp1':
            docams.extend([docams1,] if isinstance(docams1,str) else docams1)
        elif spec == 'sp2':
            docams.extend([docams2,] if isinstance(docams2,str) else docams2)
        else:
            cmd.error('text="I do not understand spectrograph: %s"'%spec)
            self.success = False

        try:
            for cam in docams:
                basename = 'sdR-%s-%08d.fit*'
                self.do_one_cam(cam,indir,basename,expnum1,expnum2,test,plot)
            if len(docams) > 1:
                self._mean_moves()
        except Exception,e:
            cmd.error('text="Collimation calculation failed! %s"'%e)
            self.success = False
    #...
    
    def doHartmann(self,cmd):
        """
        Take and reduce a pair of hartmann exposures.
        Usually apply the recommended collimator moves.
        
        cmd is the currently active Commander instance, for passing info/warn messages.
        """
        exposureIds = []
        moveMotors = "noCorrect" not in cmd.cmd.keywords
        subFrame = "noSubframe" not in cmd.cmd.keywords
        
        # Take the Hartmann exposures
        for side in 'left','right':
            window = "window=850,1400" if subFrame else ""
            ret = self.actor.cmdr.call(actor='boss', forUserCmd=cmd,
                                       cmdStr='exposure arc hartmann=%s itime=4 %s %s' % \
                                       (side,
                                       window,
                                       ("noflush" if side == "right" else "")),
                                       timeLim=90.0)
            exposureId = self.actor.models["boss"].keyVarDict["exposureId"][0]
            exposureId += 1
            exposureIds.append(exposureId)
            cmd.diag('text="got hartmann %s exposure %d"' % (side, exposureId))
            
            if ret.didFail:
                cmd.error('text="failed to take %s hartmann exposure"' % (side))
                self.success = False
        
        # now actually perform the collimation calculations
        self.collimate(exposureId)
    #...
#...
