ckan.module("s3filestore-multipart-upload", function($, _) {
    "use strict";

    return {
        options: {
            cloud: "S3",
            maxSize: 10737418240,
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

        _partNumber: 1,
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
        _parts: new Array(),

        initialize: function() {
            console.log('Called init');
            $.proxyAll(this, /_on/);
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

            var self = this;
            this._file.fileupload({
                url: '',
                type: 'PUT',
                maxSize: 10737418240,
                maxChunkSizeNew: 1048576,
                replaceFileInput: false,
                paramName: 'upload',
                multipart: false,
                processData: false,
                cache: false,
                ContentType: false,
                // formData: this._onGenerateAdditionalData,
                submit: this._onUploadFileSubmit,
                chunkbeforesend: this._onChunkBeforeSend,
                chunksend: this._onChunkSend,
                chunkdone: this._onChunkUploaded,
                add: this._onFileUploadAdd,
                progressall: this._onFileUploadProgress,
                done: this._onFinishUpload,
                fail: this._onUploadFail,
                always: this._onAnyEndedUpload
            });

            this._save.on("click", this._onSaveClick);

            // this._onCheckExistingMultipart("choose");
            (function blink() {
                $(".upload-message")
                    .fadeOut(500)
                    .fadeIn(500, blink);
            })();
        },

        _onChunkSend: function(event, data) {
            console.log('Inside chunk send with data :' + data)
            if(this._signedUrls.length >= this._partNumber){
                var url = this._signedUrls[this._partNumber-1]
                this._file.fileupload("option", "url", url);
                event.target.formAction=url;
                this._partNumber += 1;
                this._uploadedParts += 1;
                console.log('Set the preseigned url for chunk :'+this._partNumber);
                console.log('The aws url is set to :'+ url)
            }

        },

        // _onChunkBeforeSend: function(event, data) {
        //   console.log('called on chunk before send.');
        //     // var target = $(event.target);
        //     // if (this._signedUrls.length > 0) {
        //     //     var url = this._signedUrls[this._partNumber -1]
        //     //     target.fileupload("option", "url", url);
        //     //     console.log('set the fileupload url to : ' + target.fileupload("option", "url"));
        //     // } else{
        //     //     this.sandbox.notify(
        //     //         "Upload error",
        //     //         this.i18n("undefined chunk"),
        //     //         "error"
        //     //     );
        //     //     return false;
        //     // }
        //
        // },

        _onChunkUploaded: function(event, data) {
            console.log('called onChunkUploaded');
            this._parts.push({'ETag': data.jqXHR.getResponseHeader('ETag'), 'PartNumber': this._partNumber});

        },

        // _onCheckExistingMultipart: function(operation) {
        //     var self = this;
        //     var id = this._id.val();
        //     if (!id) return;
        //     this.sandbox.client.call(
        //         "POST",
        //         "s3filestore_check_multipart",
        //         { id: id },
        //         function(data) {
        //             if (!data.result) return;
        //             var upload = data.result.upload;
        //
        //             var name = upload.name.slice(upload.name.lastIndexOf("/") + 1);
        //             self._uploadId = upload.id;
        //             self._uploadSize = upload.size;
        //             self._uploadedParts = upload.parts;
        //             self._uploadName = upload.original_name;
        //             self._partNumber = self._uploadedParts + 1;
        //
        //             var current_chunk_size = self._file.fileupload(
        //                 "option",
        //                 "maxChunkSize"
        //             );
        //             var uploaded_bytes = current_chunk_size * upload.parts;
        //             self._file.fileupload("option", "uploadedBytes", uploaded_bytes);
        //
        //             self.sandbox.notify(
        //                 "Incomplete upload",
        //                 "File: " + upload.original_name + "; Size: " + self._uploadSize,
        //                 "warning"
        //             );
        //             self._onEnableResumeBtn(operation);
        //         },
        //         function(error) {
        //             console.error(error);
        //             setTimeout(function() {
        //                 self._onCheckExistingMultipart(operation);
        //             }, 2000);
        //         }
        //     );
        // },

        // _onEnableResumeBtn: function(operation) {
        //     var self = this;
        //     this.$(".btn-remove-url").remove();
        //     if (operation === "choose") {
        //         self._onDisableSave(true);
        //     }
        //     this._resumeBtn
        //         .off("click")
        //         .on("click", function(event) {
        //             switch (operation) {
        //                 case "resume":
        //                     self._save.trigger("click");
        //                     self._onDisableResumeBtn();
        //                     break;
        //                 case "choose":
        //                 default:
        //                     self._file.trigger("click");
        //                     break;
        //             }
        //         })
        //         .show();
        // },

        // _onDisableResumeBtn: function() {
        //     this._resumeBtn.hide();
        // },

        _onUploadFail: function(e, data) {
            this._onHandleError("Upload fail");
            // this._onCheckExistingMultipart("resume");
        },

        _onUploadFileSubmit: function(event, data) {
            console.log('called submit');
            if (!this._uploadId) {
                this._onDisableSave(false);
                this.sandbox.notify(
                    "Upload error",
                    this.i18n("undefined_upload_id"),
                    "error"
                );
                return false;
            }

            this._setProgressType("info", this._bar);
            this._progress.show("slow");
        },

        // _onGenerateAdditionalData: function(form) {
        //     console.log('on generate additional data');
        //     return [
        //         {
        //             name: "partNumber",
        //             value: this._partNumber
        //         },
        //         {
        //             name: "uploadId",
        //             value: this._uploadId
        //         },
        //         {
        //             name: "id",
        //             value: this._resourceId
        //         }
        //     ];
        // },

        _onAnyEndedUpload: function() {
            this._partNumber = 1;
        },

        _countChunkSize: function(fileSize, maxChunkSize) {
            var count = 0;
            console.log('filesize/maxChunSize :' + fileSize / maxChunkSize);
            if (fileSize / maxChunkSize >= 1) {
                count = Math.ceil(fileSize / maxChunkSize);
            } else {
                count = Math.floor(fileSize / maxChunkSize)
            }

            console.log('The chunk counts are : '+ count);
            return count;
        },

        _onFileUploadAdd: function(event, data) {
            var self = this;
            console.log('called onfile upload add')
            this._setProgress(0, this._bar);
            var file = data.files[0];
            var target = $(event.target);
            if (this.options.maxSize && !isNaN(parseInt(this.options.maxSize))) {
                var max_size = parseInt(this.options.maxSize);
                var file_size_gb = file.size / 1073741824;
                if (file_size_gb > max_size) {
                    this._file.val("");
                    this._onCleanUpload();
                    this.sandbox.notify(
                        "Too large file:",
                        "You selected a file larger than " +
                        max_size +
                        "GB. Contact support for an alternative upload method or select a smaller one.",
                        "error"
                    );
                    event.preventDefault();
                    throw "Too large file";
                }
            }

            var chunkSize = this._countChunkSize(
                file.size,
                target.fileupload("option", "maxChunkSize")
            );
            if(
                this._uploadName &&
                this._uploadSize &&
                this._uploadedParts !== null
            ) {
                if (this._uploadSize !== file.size || this._uploadName !== file.name) {
                    this._file.val("");
                    this._onCleanUpload();
                    this.sandbox.notify(
                        "Mismatch file",
                        "You are trying to upload wrong file. Select " +
                        this._uploadName +
                        " or delete this resource and create a new one.",
                        "error"
                    );
                    event.preventDefault();
                    throw "Wrong file";
                }

                var loaded = chunkSize * this._uploadedParts;

                // target.fileupload('option', 'uploadedBytes', loaded);
                this._onFileUploadProgress(event, {
                    total: file.size,
                    loaded: loaded
                });

                this._progress.show("slow");
                this._onDisableResumeBtn();
                console.log('calling the save trigger');
                this._save.trigger("click");

                if (loaded >= file.size) {
                    this._onFinishUpload();
                }
            }

            target.fileupload("option", "maxChunkSize", chunkSize);
            this.el.off("multipartstarted.s3filestore");
            this.el.on("multipartstarted.s3filestore", function() {
                data.submit();
            });
        },

        _onFileUploadProgress: function(event, data) {
            console.log('on file upload progress');
            var progress = 100 / (data.total / data.loaded);
            this._setProgress(progress, this._bar);
        },

        _onSaveClick: function(event, pass) {
            if (pass || !window.FileList || !this._file || !this._file.val()) {
                return;
            }
            event.preventDefault();

            var dataset_id = this.options.packageId;
            this._clickedBtn = $(event.target).attr("value");
            if (this._clickedBtn == "go-dataset") {
                this._onDisableSave(false);
                this._redirect_url = this.sandbox.url("/dataset/edit/" + dataset_id);
                window.location = this._redirect_url;
            } else {
                try {
                    $("html, body").animate({ scrollTop: 0 }, "50");
                    this._onDisableSave(true);
                    this._onDisableRemove(true);
                    this._onSaveForm();
                } catch (error) {
                    console.error(error);
                    this._onDisableSave(false);
                    this._onDisableRemove(false);
                }
            }

            // this._form.trigger('submit', true);
        },

        _onSaveForm: function() {
            console.log('Calling the save form')
            var file = this._file[0].files[0];
            var self = this;
            var formData = this._form.serializeArray().reduce(function(result, item) {
                result[item.name] = item.value;
                return result;
            }, {});
            console.log(file.name);
            formData.multipart_name = file.name;
            formData.url = file.name;
            formData.package_id = this.options.packageId;
            formData.size = file.size;
            formData.url_type = "upload";
            console.log(formData)
            var action = formData.id ? "resource_update" : "resource_create";
            var url = this._form.attr("action") || window.location.href;
            this.sandbox.client.call(
                "POST",
                action,
                formData,
                function(data) {
                    var result = data.result;
                    self._packageId = result.package_id;
                    self._resourceId = result.id;

                    self._id.val(result.id);
                    self.sandbox.notify(
                        result.id,
                        self.i18n(action, { id: result.id }),
                        "success"
                    );
                    self._onPerformUpload(file);
                },
                function(err, st, msg) {
                    self.sandbox.notify("Error", msg, "error");
                    self._onHandleError("Unable to save resource");
                }
            );
        },

        _onPerformUpload: function(file) {
            console.log('Inside onPerform upload');
            var id = this._id.val();
            var self = this;
            if (this._uploadId === null)
                this._onPrepareUpload(file, id).then(
                    function(data) {
                           self._signedUrls = data.result.signed_urls;
                           self._s3UploadId = data.result.upload_id;
                            if(self._signedUrls.length > 0) {
                                var url = self._signedUrls[self._partNumber-1]
                                self._file.fileupload("option", "url", url);
                                console.log('Set the preseigned url');
                            }
                            else{
                                this.sandbox.notify(
                                    "Upload error",
                                    this.i18n("Upload failed, check signed urls."),
                                    "error"
                                );
                                return false;
                            }
                           self._uploadId = data.result.id;
                           self.el.trigger("multipartstarted.s3filestore");
                    },
                    function(err) {
                        console.error(err);
                        self._onHandleError("Unable to initiate multipart upload");
                    }
                );
            else this.el.trigger("multipartstarted.s3filestore");
        },

        _onPrepareUpload: function(file, id) {
            console.log('Inside onPrepareUpload');
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
            var self = this;
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

        _onFinishUpload: function(event, data) {
            console.log('Finished upload data: ' + data)
            var self = this;
            console.log('Etag for Part: ' + data.jqXHR.getResponseHeader('Etag'))
            var data_dict = {
                uploadId: self._uploadId,
                id: self._resourceId,
                name: 'test',
                save_action: self._clickedBtn,
                parts: self._parts,
                S3upload_id: self._s3UploadId
            };
            this.sandbox.client.call(
                "POST",
                "s3filestore_finish_multipart",
                data_dict,
                function(data) {
                    self._progress.hide("fast");
                    self._onDisableSave(false);

                    if (self._resourceId && self._packageId) {
                        self.sandbox.notify(
                            "Success",
                            self.i18n("upload_completed"),
                            "success"
                        );
                        // self._form.remove();
                        if (self._clickedBtn == "again") {
                            this._redirect_url = self.sandbox.url(
                                "/dataset/new_resource/" + self._packageId
                            );
                        } else {
                            this._redirect_url = self.sandbox.url(
                                "/dataset/" + self._packageId
                            );
                        }
                        self._form.attr("action", this._redirect_url);
                        self._form.attr("method", "GET");
                        self.$("[name]").attr("name", null);
                        setTimeout(function() {
                            self._form.submit();
                        }, 3000);
                    }
                },
                function(err) {
                    console.error(err);
                    self._onHandleError(self.i18n("unable_to_complete_upload"));
                }
            );
            self._progress.hide("fast");
            self._onDisableSave(false);

            if (self._resourceId && self._packageId) {
                self.sandbox.notify(
                    "Success",
                    self.i18n("upload_completed"),
                    "success"
                );
                // self._form.remove();
                if (self._clickedBtn == "again") {
                    this._redirect_url = self.sandbox.url(
                        "/dataset/new_resource/" + self._packageId
                    );
                } else {
                    this._redirect_url = self.sandbox.url(
                        "/dataset/" + self._packageId
                    );
                }
                self._form.attr("action", this._redirect_url);
                self._form.attr("method", "GET");
                self.$("[name]").attr("name", null);
                setTimeout(function() {
                    self._form.submit();
                }, 3000);
            }
            this._setProgressType("success", this._bar);
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
            this.sandbox.notify("Error", msg, "error");
            this._onDisableSave(false);
        },

        _onCleanUpload: function() {
            this.$(".btn-remove-url").trigger("click");
        }
    };
});
