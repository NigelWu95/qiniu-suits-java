package com.qiniu.model.media;

public class Avinfo {

    private Format format;
    private AudioStream audioStream;
    private VideoStream videoStream;

    public AudioStream getAudioStream() {
        return audioStream;
    }

    public void setAudioStream(AudioStream audioStream) {
        this.audioStream = audioStream;
    }

    public VideoStream getVideoStream() {
        return videoStream;
    }

    public void setVideoStream(VideoStream videoStream) {
        this.videoStream = videoStream;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }
}