#!/bin/bash -x

wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz

wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz.md5

md5sum -c ffmpeg-release-amd64-static.tar.xz.md5

tar xvf ffmpeg-release-amd64-static.tar.xz

mkdir -p ffmpeg/bin

cp ffmpeg-*-amd64-static/ffmpeg ffmpeg/bin/

cd ffmpeg

zip -r ../ffmpeg_layer.zip .
rm -fr ffmpeg/
