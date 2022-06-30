ckan.module("s3filestore-multipart-upload", function($, _) {
    "use strict";

    return {
        options: {
            cloud: "S3",
            filePartMaxSize: 0,
            fileMaxSize: 0,
            i18n: {
                resource_create: _("Resource has been created."),
                resource_update: _("Resource has been updated."),
                undefined_upload_id: _("Undefined uploadId."),
                upload_completed: _(
                    "Upload completed. You will be redirected in few seconds..."
                ),
                unable_to_finish: _("Unable to finish multipart upload")
            }
        },

        _partNumber: 0,
        _s3UploadId: null,
        _uploadId: null,
        _packageId: null,
        _resourceId: null,
        _uploadSize: null,
        _uploadName: null,
        _uploadedParts: null,
        _clickedBtn: null,
        _redirect_url: null,
        _signedUrls: null,
        _originalFileSize: null,
        _parts: new Array(),
        _blobs: new Array(),
        _fileParts: new Array(),
        _blobSlice: null,

        initialize: function() {
            $.proxyAll(this, /_on/);
            this._blobSlice = $.support.blobSlice && function () {
                const slice = this.slice || this.webkitSlice || this.mozSlice;
                return slice.apply(this, arguments);
            }
            // There is an undescore as a prefix added to package ID in
            // order to prevent type-coercion, so we have to strip it
            this.options.packageId = this.options.packageId.slice(1);
            this._form = this.$("form");
            this._file = $("#field-image-upload");
            this._url = $("#field-image-url");
            this._save = $("[name=save]");
            this._id = $("input[name=id]");
            this._progress = $("<div>", {
                class: "progress"
            });

            this._bar = $("<div>", {
                class: "progress-bar progress-bar-striped progress-bar-animated active"
            });
            this._progress.append(this._bar);
            this._progress.insertAfter(this._url.parent().parent());
            this._progress.hide();

            this._resumeBtn = $("<a>", { class: "btn btn-info controls" })
                .insertAfter(this._progress)
                .text("Resume Upload");
            this._resumeBtn.hide();

            this._save.on("click", this._onSaveClick);

            (function blink() {
                $(".upload-message")
                    .fadeOut(500)
                    .fadeIn(500, blink);
            })();
        },

        _onSaveClick: function(event, pass) {
            if (pass || !window.FileList || !this._file || !this._file.val()) {
                return;
            }
            event.preventDefault();
            const dataset_id = this.options.packageId;
            this._clickedBtn = $(event.target).attr("value");
            if (this._clickedBtn == "go-dataset") {
                this._onDisableSave(false);
                this._redirect_url = this.sandbox.url("/dataset/edit/" + dataset_id);
                window.location = this._redirect_url;
            } else {
                try {
                    $("html, body").animate({ scrollTop: 0 }, "50");
                    this._onSaveForm();
                } catch (error) {
                    console.error(error);
                    this._onDisableSave(false);
                    this._onDisableRemove(false);
                }
            }
        },

        _onFormatBytes(bytes, decimals=2){
            if (bytes === 0) return '0 Bytes';

            const k = 1024;
            const dm = decimals < 0 ? 0 : decimals;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

            const i = Math.floor(Math.log(bytes) / Math.log(k));

            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];

        },

        _onSaveForm: function() {
            const file = this._file[0].files[0];
            const self = this;
            // Check if file size is above the max allowed limit.
            if (this.options.fileMaxSize && !isNaN(parseInt(this.options.fileMaxSize))) {
                const max_size = parseInt(this.options.fileMaxSize);
                if (file.size > max_size) {
                    this._file.val("");
                    this._onCleanUpload();
                    self.sandbox.notify(
                        "Large file:",
                        "You selected a file larger than " +
                        self._onFormatBytes(max_size) +
                        ". Contact support for an alternative upload method or select a smaller one.",
                        "error"
                    );
                    throw "Large file";
                }
            }
            $('.alert-error').remove();
            this._setProgressType("info", this._bar);
            this._progress.show("slow");
            this._setProgress(0, this._bar);
            this._onDisableSave(true);
            this._onDisableRemove(true);

            const formData = this._form.serializeArray().reduce(function(result, item) {
                result[item.name] = item.value;
                return result;
            }, {});
            formData.multipart_name = file.name;
            formData.url = file.name;
            formData.package_id = this.options.packageId;
            formData.size = file.size;
            formData.url_type = "upload";
            const action = formData.id ? "resource_update" : "resource_create";
            self.sandbox.client.call(
                "POST",
                action,
                formData,
                function(data) {
                    const result = data.result;
                    self._packageId = result.package_id;
                    self._resourceId = result.id;

                    self._id.val(result.id);
                    self._onPerformChunkedUpload(file);
                },
                function(err, st, msg) {
                    self.sandbox.notify("Error", msg, "error");
                    self._onHandleError("Unable to save resource");
                }
            );
        },

        _onPerformChunkedUpload: function(file) {
            const id = this._id.val();
            const self = this;
            const originalFileSize = self._originalFileSize = file.size;
            if (this._uploadId === null)
                this._onPrepareUpload(file, id).then(
                    function(data) {
                        const blobs = self._blobs = [];
                        let start = 0;
                        let end, blob;
                        if (start > file.size) {
                            this._onFinishUpload();
                            return;
                        }
                        while(start < file.size) {
                            start = self.options.filePartMaxSize  * self._partNumber++
                            end = Math.min(start + self.options.filePartMaxSize, file.size);
                            if (file.slice(start, end).size > 0){
                                blobs.push(file.slice(start, end, file.type))
                            }
                        }
                        for (let i = 0; i < blobs.length; i++) {
                            blob = blobs[i];
                            self._fileParts.push({'url': data.result.signed_urls[i],
                                           'name': file.name,
                                           'uploadId': data.result.id,
                                           'id': id,
                                           'data': blob,
                                           'partNumber': i+1,
                                           'S3uploadId': data.result.upload_id,
                                           'originalFileSize': originalFileSize});
                        }
                        self._onSendAllFileParts();
                    },
                    function(err) {
                        console.error(err);
                        self._onHandleError("Unable to initiate multipart upload");
                    }
                );
        },

        _onSendAllFileParts: function(){
            const self = this;
            let progress=0;
            const parts = new Array();
            const fileParts = self._fileParts;
            const name = fileParts[0]['name'];
            const uploadId = fileParts[0]['uploadId'];
            const s3UploadId = fileParts[0]['S3uploadId'];
            for (let i = 0; i < fileParts.length; i++) {
                if(fileParts[i]['url'] !== undefined) {
                    const request = self.uploadXHR = new XMLHttpRequest();
                    if (this._bar.attr("listener") === undefined){
                        request.upload.addEventListener("progress", function(e){
                            progress = ((e.loaded / e.total) * 100);
                            self._setProgress(progress, self._bar);
                        });
                        this._bar.attr("listener", "true");
                    }
                    request.open('PUT', fileParts[i]['url'], true);
                    // Apply this header only if the file is not a chunked upload.
                    if (fileParts.length === 1) {
                        request.setRequestHeader('x-amz-storage-class', 'INTELLIGENT_TIERING')
                    }
                    request.send(fileParts[i]['data']);
                    request.onload = function() {
                        // Stitch the file only if there is more than one chunk.
                        if(fileParts.length > 1) {
                            const startIndex = request.responseURL.indexOf('partNumber=')+11;
                            const endIndex = request.responseURL.indexOf('&uploadId')
                            const partNumber = request.responseURL.substr(startIndex, endIndex-startIndex)
                            parts.push({'ETag': JSON.parse(request.getResponseHeader('ETag')), 'PartNumber': parseInt(partNumber) })
                            // Call API to stitch the parts
                            if(parts.length === fileParts.length){
                                const data_dict = {
                                    resourceId: uploadId,
                                    name: name,
                                    S3uploadId: s3UploadId,
                                    parts: parts
                                };
                                self.sandbox.client.call(
                                    "POST",
                                    "s3filestore_finish_multipart",
                                    data_dict,
                                    function(data) {
                                        if (uploadId) {
                                            self._onFinishUpload();
                                        }
                                    },
                                    function(err) {
                                        console.error(err);
                                        self._onHandleError(self.i18n("Unable to complete multipart upload"));
                                    }
                                );
                            }
                        }
                        else{
                            self._onFinishUpload();
                        }

                    }
                    request.onerror = function() {
                       self._onHandleError("Error uploading file.")
                    }
                }
            }


        },

        _onPrepareUpload: function(file, id) {
            return $.ajax({
                method: "POST",
                url: this.sandbox.client.url(
                    "/api/action/s3filestore_initiate_multipart"
                ),
                data: JSON.stringify({
                    id: id,
                    name: encodeURIComponent(file.name),
                    fileSize: file.size,
                    type: file.type
                })
            });
        },

        _onAbortUpload: function(id) {
            const self = this;
            this.sandbox.client.call(
                "POST",
                "s3filestore_abort_multipart",
                {
                    id: id
                },
                function(data) {
                    console.log(data);
                },
                function(err) {
                    console.error(err);
                    self._onHandleError("Unable to abort multipart upload");
                }
            );
        },

        _onFinishUpload: function() {
            const self = this;
            self._setProgress(100, self._bar);
            self._progress.hide("fast");
            self._onDisableSave(false);
            self.sandbox.notify(
                "Success",
                self.i18n("upload_completed"),
                "success"
            );
            // self._form.remove();
            if(this._clickedBtn === "again") {
                this._redirect_url = self.sandbox.url(
                    "/dataset/new_resource/" + self._packageId
                );
            } else {
                self._redirect_url = self.sandbox.url(
                    "/dataset/" + self._packageId
                );
            }
            self._form.attr("action", self._redirect_url);
            self._form.attr("method", "GET");
            self.$("[name]").attr("name", null);
            setTimeout(function() {
                self._form.submit();
            }, 2000);
        },

        _onDisableSave: function(value) {
            this._save.attr("disabled", value);
        },

        _onDisableRemove: function(value) {
            $(".btn-remove-url").attr("disabled", value);
            if (value) {
                $(".btn-remove-url").off();
            } else {
                $(".btn-remove-url").on();
            }
        },

        _setProgress: function(progress, bar) {
            bar.css("width", progress + "%");
        },

        _setProgressType: function(type, bar) {
            bar
                .removeClass("bg-success bg-info bg-warning bg-danger")
                .addClass("bg-" + type);
        },

        _onHandleError: function(msg) {
            const self = this;
            const dict = {'id': this._resourceId}
            this.sandbox.notify("Error", msg, "error");
            // this.sandbox.client.call(
            //     "POST",
            //     "resource_delete",
            //     dict,
            //     function(data) {},
            //     function(err) {
            //         self.sandbox.notify("Error", "Unable to rollback resource: "+this._resourceId, "error");
            //     }
            //
            // )
            this._onDisableSave(false);
        },

        _onCleanUpload: function() {
            this.$(".btn-remove-url").trigger("click");
        }
    };
});
